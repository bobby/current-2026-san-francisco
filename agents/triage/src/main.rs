//! The triage agent (Scenario 1, docs/design.md §8.1): turns the alert
//! firehose into a few enriched signals.
//!
//! - It consumes `platform.alerts` and groups firing alerts per job
//!   (`grouping`).
//! - For each group, code gathers evidence from the ops DB and Prometheus
//!   (`evidence`) and Jev answers five questions about it in one request
//!   (`questions`).
//! - Code policy turns the answers into `suppress`, `attach` to an open
//!   incident, or `raise` (`policy`, thresholds in `triage.yaml`).
//! - Every verdict goes to `platform.signals`, with its state, questions and
//!   answers. Raised signals get a card in `#platform-signals`; attached ones
//!   go in the incident's thread.
//!
//! `triage replay --from … --to …` re-judges the alerts of a past window from
//! the ops DB, with no side effects, for tuning.

mod agent;
mod config;
mod evidence;
mod grouping;
mod incidents;
mod policy;
mod questions;
mod summary;

use std::{
    collections::HashSet,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use agent::{Outputs, Stats, Triage};
use anyhow::Context;
use axum::{Json, Router, extract::State, routing::get};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use config::Config;
use evidence::Sources;
use grouping::{Grouper, Watch};
use incidents::Incidents;
use platform_clients::{chatops::ChatopsClient, kafka::Publisher, ops_db::OpsDb, prometheus::PrometheusClient};
use platform_model::{AlertEvent, AlertStatus, Disposition, topics};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use serde_json::json;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tracing::{info, warn};
use typesafe_ai::ReqwestClient;

#[derive(Debug, Parser)]
#[command(about = "The triage agent: turns the alert firehose into a few enriched signals")]
struct Cli {
    /// The tuning: windows and policy thresholds.
    #[arg(long, env = "TRIAGE_CONFIG", default_value = "agents/triage/triage.yaml", global = true)]
    config: PathBuf,
    #[arg(long, env = "OPS_DATABASE_URL", default_value = "postgres://ops:ops@localhost:5432/ops", global = true)]
    ops_database_url: String,
    #[arg(long, env = "PROMETHEUS_URL", default_value = "http://localhost:9090", global = true)]
    prometheus_url: String,
    /// Jev's API (TypeSafe's, or an OpenJev/SemIf server with the same API).
    #[arg(long, env = "JEV_BASE_URL", default_value = "https://api.typesafe.ai", global = true)]
    jev_base_url: String,
    #[arg(long, env = "JEV_API_KEY", hide_env_values = true, global = true)]
    jev_api_key: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Triage the live alert stream (the default).
    Run {
        #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS", default_value = "localhost:9094")]
        kafka_bootstrap_servers: String,
        #[arg(long, env = "CHATOPS_URL", default_value = "http://localhost:8092")]
        chatops_url: String,
        #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8093")]
        listen_addr: SocketAddr,
    },
    /// Re-judge the alerts of a past window from the ops DB, printing each
    /// verdict. Publishes and posts nothing.
    Replay {
        #[arg(long)]
        from: DateTime<Utc>,
        #[arg(long)]
        to: DateTime<Utc>,
        /// Only these jobs.
        #[arg(long)]
        job: Vec<String>,
        /// Print each judgment's evidence too.
        #[arg(long)]
        evidence: bool,
        /// Also write each signal, as JSON Lines, to this file (to compare
        /// question or threshold changes offline).
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let replaying = matches!(cli.command, Some(Command::Replay { .. }));
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    if replaying {
        tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr).init();
    } else {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    }
    let config = Config::load(&cli.config)?;
    let command = cli.command.unwrap_or(Command::Run {
        kafka_bootstrap_servers: "localhost:9094".into(),
        chatops_url: "http://localhost:8092".into(),
        listen_addr: "0.0.0.0:8093".parse()?,
    });
    let jev = match cli.jev_api_key.filter(|k| !k.is_empty()) {
        Some(key) => Some(ReqwestClient::with_config(
            key,
            typesafe_ai::ClientConfig {
                base_url: cli.jev_base_url.clone(),
                timeout: Duration::from_secs(20),
                ..Default::default()
            },
        )?),
        None => None,
    };
    let sources = Sources {
        db: OpsDb::connect(&cli.ops_database_url).await.context("connecting to the ops DB")?,
        prometheus: PrometheusClient::new(&cli.prometheus_url),
    };
    match command {
        Command::Run { kafka_bootstrap_servers, chatops_url, listen_addr } => {
            let Some(jev) = jev else {
                // Without a key there's nothing to judge with: stay up (so the
                // stack is healthy) and say so.
                warn!("JEV_API_KEY isn't set; not triaging");
                return serve(listen_addr, Arc::new(Stats::default()), false).await;
            };
            let since = Utc::now() - config.incident_ttl;
            let recent = sources.db.signals_between(None, since, Utc::now() + chrono::Duration::minutes(1)).await?;
            let incidents = Incidents::from_signals(config.incident_ttl, &recent);
            let triage = Arc::new(Triage {
                outputs: Some(Outputs {
                    publisher: Publisher::new(&kafka_bootstrap_servers, "triage")?,
                    chatops: ChatopsClient::new(&chatops_url),
                }),
                config,
                sources,
                jev,
                incidents: Mutex::new(incidents),
                stats: Stats::default(),
            });
            run(triage, &kafka_bootstrap_servers, listen_addr).await
        }
        Command::Replay { from, to, job, evidence, out } => {
            let jev = jev.context("JEV_API_KEY isn't set")?;
            let triage = Triage {
                incidents: Mutex::new(Incidents::new(config.incident_ttl)),
                config,
                sources,
                jev,
                outputs: None,
                stats: Stats::default(),
            };
            replay(&triage, from, to, &job, evidence, out.as_deref()).await
        }
    }
}

async fn serve(addr: SocketAddr, stats: Arc<Stats>, enabled: bool) -> anyhow::Result<()> {
    let app =
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route(
                "/stats",
                get(move |State(stats): State<Arc<Stats>>| async move {
                    Json(json!({"enabled": enabled, "stats": &*stats}))
                }),
            )
            .with_state(stats);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, enabled, "triage listening");
    axum::serve(listener, app).await.context("HTTP server")
}

/// Consumes the alert stream, groups it, and judges each group when due,
/// at most `max_concurrent` at a time and one at a time per job.
async fn run(triage: Arc<Triage>, bootstrap_servers: &str, listen_addr: SocketAddr) -> anyhow::Result<()> {
    let stats = Arc::new(Stats::default());
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "triage")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
        .context("creating the Kafka consumer")?;
    consumer.subscribe(&[topics::ALERTS])?;
    info!(topic = topics::ALERTS, "triaging alerts");
    let server = tokio::spawn(serve(listen_addr, stats.clone(), true));

    let mut grouper = Grouper::new(triage.config.group_window);
    let mut watch = Watch::new(triage.config.recheck);
    let mut busy: HashSet<String> = HashSet::new();
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<(String, Option<Disposition>)>();
    let permits = Arc::new(Semaphore::new(triage.config.max_concurrent));
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            message = consumer.recv() => {
                let message = match message {
                    Ok(m) => m,
                    Err(err) => {
                        warn!(%err, "Kafka consumer error");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                };
                let Some(payload) = message.payload() else { continue };
                let alert: AlertEvent = match serde_json::from_slice(payload) {
                    Ok(a) => a,
                    Err(err) => {
                        warn!(%err, offset = message.offset(), "skipping an undecodable alert");
                        continue;
                    }
                };
                let Some(job) = alert.job_name.clone() else { continue };
                watch.observe(&alert);
                if alert.status == AlertStatus::Firing {
                    triage.stats.alerts_in.fetch_add(1, Ordering::Relaxed);
                    grouper.push(&job, alert, Utc::now());
                }
            }
            _ = tick.tick() => {
                let now = Utc::now();
                let mut due: Vec<(String, Vec<AlertEvent>)> =
                    grouper.due(now, &busy).into_iter().map(|d| (d.job_name, d.alerts)).collect();
                // A job whose new group is due now isn't rechecked as well.
                let dispatching: HashSet<String> = busy.iter().chain(due.iter().map(|(j, _)| j)).cloned().collect();
                due.extend(watch.due(now, &dispatching, &grouper));
                for (job, alerts) in due {
                    busy.insert(job.clone());
                    spawn_judgment(triage.clone(), permits.clone(), done_tx.clone(), job, alerts);
                }
                copy_stats(&triage.stats, &stats);
            }
            Some((job, disposition)) = done_rx.recv() => {
                if let Some(d) = disposition {
                    watch.judged(&job, Utc::now(), d);
                }
                busy.remove(&job);
            }
        }
        if server.is_finished() {
            return server.await?;
        }
    }
}

/// Settles, judges and acts on one group in the background, then reports
/// the verdict's disposition (none on an error) on `done`.
fn spawn_judgment(
    triage: Arc<Triage>,
    permits: Arc<Semaphore>,
    done: mpsc::UnboundedSender<(String, Option<Disposition>)>,
    job: String,
    alerts: Vec<AlertEvent>,
) {
    tokio::spawn(async move {
        // A failure (Jev's API timing out, say) is retried, so the alerts
        // aren't lost; the evidence is gathered afresh each time.
        let mut result = Err(anyhow::anyhow!("not judged"));
        for attempt in 1..=4u32 {
            result = match triage.settle(&job).await {
                Ok(()) => {
                    let _permit = permits.acquire().await;
                    triage.handle(&job, &alerts).await
                }
                Err(err) => Err(err),
            };
            match &result {
                Err(err) if attempt < 4 => {
                    warn!(job, attempt, error = format!("{err:#}"), "judging alerts; retrying");
                    tokio::time::sleep(Duration::from_secs(15 * u64::from(attempt))).await;
                }
                _ => break,
            }
        }
        let disposition = match result {
            Ok(v) => {
                info!(
                    job,
                    alerts = alerts.len(),
                    disposition = ?v.signal.disposition,
                    class = ?v.judgment.failure_class,
                    actionable = v.judgment.actionable,
                    impact = v.judgment.user_impact,
                    "judged"
                );
                Some(v.signal.disposition)
            }
            Err(err) => {
                triage.stats.errors.fetch_add(1, Ordering::Relaxed);
                warn!(job, error = format!("{err:#}"), "judging alerts");
                None
            }
        };
        let _ = done.send((job, disposition));
    });
}

fn copy_stats(from: &Stats, to: &Stats) {
    for (f, t) in [
        (&from.alerts_in, &to.alerts_in),
        (&from.judgments, &to.judgments),
        (&from.raised, &to.raised),
        (&from.attached, &to.attached),
        (&from.suppressed, &to.suppressed),
        (&from.alerts_suppressed, &to.alerts_suppressed),
        (&from.errors, &to.errors),
    ] {
        t.store(f.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// Re-judges a past window's alerts as the live agent would have: grouped,
/// settled and rechecked on a simulated clock that ticks every 15 seconds.
async fn replay(
    triage: &Triage,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    jobs: &[String],
    print_evidence: bool,
    out: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    let mut out = out.map(std::fs::File::create).transpose()?;
    let wanted = |a: &AlertEvent| a.job_name.as_ref().is_some_and(|j| jobs.is_empty() || jobs.contains(j));
    let alerts: Vec<AlertEvent> =
        triage.sources.db.alerts_between(None, from, to).await?.into_iter().filter(|a| wanted(a)).collect();
    let mut grouper = Grouper::new(triage.config.group_window);
    let mut watch = Watch::new(triage.config.recheck);
    let (mut raised, mut attached, mut suppressed, mut alerts_in) = (0, 0, 0, 0);
    let mut verdicts = Vec::new();
    let mut next = alerts.iter().peekable();
    let mut now = from;
    while now <= to + chrono::Duration::from_std(triage.config.group_window)? {
        while let Some(a) = next.next_if(|a| a.ts <= now) {
            watch.observe(a);
            if a.status == AlertStatus::Firing {
                alerts_in += 1;
                grouper.push(a.job_name.as_deref().unwrap_or_default(), a.clone(), a.ts);
            }
        }
        let mut due: Vec<(String, Vec<AlertEvent>, DateTime<Utc>)> =
            grouper.due(now, &HashSet::new()).into_iter().map(|d| (d.job_name, d.alerts, d.due)).collect();
        let dispatching: HashSet<String> = due.iter().map(|(j, _, _)| j.clone()).collect();
        due.extend(watch.due(now, &dispatching, &grouper).into_iter().map(|(j, a)| (j, a, now)));
        for (job, group, due_at) in due {
            let at = triage.sources.settled(&triage.config, &job, due_at, Utc::now()).await?.unwrap_or(due_at);
            let v = match triage.judge(&job, &group, &verdicts, at).await {
                Ok(v) => v,
                Err(err) => {
                    println!("{}  {job}  ERROR {err:#}", at.format("%H:%M:%S"));
                    continue;
                }
            };
            let s = &v.signal;
            match s.disposition {
                Disposition::Raise => raised += 1,
                Disposition::Attach => attached += 1,
                Disposition::Suppress => suppressed += 1,
            }
            let j = &v.judgment;
            println!(
                "{}  {:<40} {:<8} act {:.2} {:<24} ({:.2}) impact {:.1} deploy {} same {}  | {}",
                at.format("%H:%M:%S"),
                s.job_name,
                format!("{:?}", s.disposition).to_lowercase(),
                j.actionable,
                questions::class_name(j.failure_class),
                j.class_probability(j.failure_class),
                j.user_impact,
                j.deploy_related.map_or("-".into(), |p| format!("{p:.2}")),
                j.same_incident.as_ref().map_or("-".into(), |(c, p)| format!("{c} {p:.2}")),
                v.summary,
            );
            if print_evidence {
                println!("{}", serde_json::to_string_pretty(&s.judgment["state"])?);
            }
            if let Some(f) = out.as_mut() {
                use std::io::Write;
                writeln!(f, "{}", serde_json::to_string(s)?)?;
            }
            watch.judged(&job, at, s.disposition);
            triage.incidents.lock().await.record(s, &v.summary, None);
            verdicts.push(v.signal);
        }
        now += chrono::Duration::seconds(15);
    }
    println!(
        "\n{alerts_in} alerts in; {} judgments: {raised} raised, {attached} attached, {suppressed} suppressed",
        raised + attached + suppressed
    );
    Ok(())
}
