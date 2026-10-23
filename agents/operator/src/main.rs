//! The operator agent (Scenario 3, docs/design.md §8.3): handles routine
//! incidents end to end and escalates only when necessary.
//!
//! - It consumes `platform.signals` and opens an incident for each raised
//!   signal, with a card in `#platform-incidents` and a ticket.
//! - Each incident runs an explicit state machine (`incident`): gather the
//!   facts (`facts`), ask Jev for a remediation (`questions`), check it
//!   against the guardrails (`guardrails`, policy in `operator.yaml`), act
//!   through cicd-sim or the Flink REST API, verify with Jev, then resolve
//!   or escalate.
//! - It also consumes `platform.actions`, for people's decisions on its
//!   escalation cards.
//!
//! `operator decide --from … --to …` re-decides past raised signals from the
//! ops DB and prints what the operator would do, with no side effects.

mod config;
mod facts;
mod guardrails;
mod incident;
mod notes;
mod questions;

use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{Json, Router, extract::State, routing::get};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use config::Config;
use facts::Sources;
use incident::{Operator, Outputs, Stats};
use platform_clients::{
    chatops::ChatopsClient, cicd::CicdClient, flink::FlinkClient, kafka::Publisher, ops_db::OpsDb,
    prometheus::PrometheusClient, tickets::TicketsClient,
};
use platform_model::{ActionEvent, Disposition, Signal, topics};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{info, warn};
use typesafe_ai::ReqwestClient;

#[derive(Debug, Parser)]
#[command(about = "The operator agent: remediates raised signals within guardrails, and escalates the rest")]
struct Cli {
    /// The tuning: guardrails and windows.
    #[arg(long, env = "OPERATOR_CONFIG", default_value = "agents/operator/operator.yaml", global = true)]
    config: PathBuf,
    #[arg(long, env = "OPS_DATABASE_URL", default_value = "postgres://ops:ops@localhost:5432/ops", global = true)]
    ops_database_url: String,
    #[arg(long, env = "PROMETHEUS_URL", default_value = "http://localhost:9090", global = true)]
    prometheus_url: String,
    #[arg(long, env = "FLINK_URL", default_value = "http://localhost:8081", global = true)]
    flink_url: String,
    #[arg(long, env = "CICD_URL", default_value = "http://localhost:8091", global = true)]
    cicd_url: String,
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
    /// Handle the live signal stream (the default).
    Run {
        #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS", default_value = "localhost:9094")]
        kafka_bootstrap_servers: String,
        #[arg(long, env = "CHATOPS_URL", default_value = "http://localhost:8092")]
        chatops_url: String,
        #[arg(long, env = "TICKETS_URL", default_value = "http://localhost:8094")]
        tickets_url: String,
        #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8095")]
        listen_addr: SocketAddr,
    },
    /// Re-decide the raised signals of a past window from the ops DB,
    /// printing Jev's pick and the guardrails' verdict. Acts on nothing.
    /// The catalogue and the cluster's capacity are as of now.
    Decide {
        #[arg(long)]
        from: DateTime<Utc>,
        #[arg(long)]
        to: DateTime<Utc>,
        /// Only these jobs.
        #[arg(long)]
        job: Vec<String>,
        /// Print each decision's state too.
        #[arg(long)]
        evidence: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx=warn".into());
    if matches!(cli.command, Some(Command::Decide { .. })) {
        tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr).init();
    } else {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    }
    let config = Config::load(&cli.config)?;
    let command = cli.command.unwrap_or(Command::Run {
        kafka_bootstrap_servers: "localhost:9094".into(),
        chatops_url: "http://localhost:8092".into(),
        tickets_url: "http://localhost:8094".into(),
        listen_addr: "0.0.0.0:8095".parse()?,
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
        flink: FlinkClient::new(&cli.flink_url),
        cicd: CicdClient::new(&cli.cicd_url),
    };
    match command {
        Command::Run { kafka_bootstrap_servers, chatops_url, tickets_url, listen_addr } => {
            let Some(jev) = jev else {
                // Without a key there's nothing to decide with: stay up (so
                // the stack is healthy) and say so.
                warn!("JEV_API_KEY isn't set; not operating");
                let operator = Arc::new(operator(config, sources, None, None));
                return serve(listen_addr, operator, false).await;
            };
            let outputs = Outputs {
                publisher: Publisher::new(&kafka_bootstrap_servers, "operator")?,
                chatops: ChatopsClient::new(&chatops_url),
                tickets: TicketsClient::new(&tickets_url, "operator-bot"),
            };
            let operator = Arc::new(operator(config, sources, Some(jev), Some(outputs)));
            run(operator, &kafka_bootstrap_servers, listen_addr).await
        }
        Command::Decide { from, to, job, evidence } => {
            let jev = jev.context("JEV_API_KEY isn't set")?;
            let operator = operator(config, sources, Some(jev), None);
            decide(&operator, from, to, &job, evidence).await
        }
    }
}

fn operator(config: Config, sources: Sources, jev: Option<ReqwestClient>, outputs: Option<Outputs>) -> Operator {
    Operator {
        config,
        sources,
        // A placeholder client when disabled: it's never called.
        jev: jev.unwrap_or_else(|| ReqwestClient::new("disabled").expect("HTTP client")),
        outputs,
        records: Mutex::new(BTreeMap::new()),
        approvals: Mutex::new(BTreeMap::new()),
        stats: Stats::default(),
    }
}

/// `/health`, `/stats` (counters) and `/incidents` (each incident's stage
/// and outcome).
async fn serve(addr: SocketAddr, operator: Arc<Operator>, enabled: bool) -> anyhow::Result<()> {
    let app =
        Router::new()
            .route("/health", get(|| async { "ok" }))
            .route(
                "/stats",
                get(move |State(op): State<Arc<Operator>>| async move {
                    Json(json!({"enabled": enabled, "stats": &op.stats}))
                }),
            )
            .route(
                "/incidents",
                get(|State(op): State<Arc<Operator>>| async move {
                    Json(op.records.lock().await.values().cloned().collect::<Vec<_>>())
                }),
            )
            .with_state(operator);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, enabled, "operator listening");
    axum::serve(listener, app).await.context("HTTP server")
}

/// Consumes raised signals, and decisions on the operator's escalations.
async fn run(operator: Arc<Operator>, bootstrap_servers: &str, listen_addr: SocketAddr) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "operator")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
        .context("creating the Kafka consumer")?;
    consumer.subscribe(&[topics::SIGNALS, topics::ACTIONS])?;
    info!(topics = ?[topics::SIGNALS, topics::ACTIONS], "operating");
    let server = tokio::spawn(serve(listen_addr, operator.clone(), true));
    let mut tick = tokio::time::interval(Duration::from_secs(5));
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
                match message.topic() {
                    topics::SIGNALS => match serde_json::from_slice::<Signal>(payload) {
                        Ok(signal) => operator.on_signal(signal).await,
                        Err(err) => warn!(%err, "skipping an undecodable signal"),
                    },
                    topics::ACTIONS => match serde_json::from_slice::<ActionEvent>(payload) {
                        Ok(event) => {
                            let operator = operator.clone();
                            tokio::spawn(async move { operator.on_action(event).await });
                        }
                        Err(err) => warn!(%err, "skipping an undecodable action"),
                    },
                    _ => {}
                }
            }
            _ = tick.tick() => {
                if server.is_finished() {
                    return server.await?;
                }
            }
        }
    }
}

/// Re-decides a past window's raised signals, as of each signal's time.
async fn decide(
    operator: &Operator,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    jobs: &[String],
    print_state: bool,
) -> anyhow::Result<()> {
    let signals: Vec<Signal> = operator
        .sources
        .db
        .signals_between(None, from, to)
        .await?
        .into_iter()
        .filter(|s| s.disposition == Disposition::Raise && (jobs.is_empty() || jobs.contains(&s.job_name)))
        .collect();
    let g = &operator.config.guardrails;
    for signal in &signals {
        let situation = operator.sources.situation(&operator.config.context, signal, signal.ts).await?;
        let decision = operator.decide(&situation).await?;
        let assessment = guardrails::assess(g, &decision, &situation, 0, 0, operator.config.observe_max);
        let verdict = match &assessment.verdict {
            guardrails::Verdict::Act(plan) => format!("act: {}", plan.describe()),
            guardrails::Verdict::Observe => "observe".into(),
            guardrails::Verdict::Escalate { to } => format!("escalate to {to:?}").to_lowercase(),
        };
        println!(
            "{}  {:<40} {:<24} → {:<28} ({:.2})  {verdict}{}",
            signal.ts.format("%H:%M:%S"),
            signal.job_name,
            notes::class_label(signal.failure_class),
            decision.remediation.name(),
            decision.probability,
            if assessment.failures().is_empty() { String::new() } else { format!(" | {}", assessment.failures()) },
        );
        if print_state {
            println!("{}", serde_json::to_string_pretty(&situation.state)?);
        }
    }
    println!("\n{} raised signals", signals.len());
    Ok(())
}
