//! The director (docs/design.md §10.2): makes the demo repeatable.
//!
//! - `reset`: stop the fault injectors and the agents, cancel every job and
//!   skip its consumer group (and the agents') to the latest offsets, empty
//!   the ops history and the tickets, forget the Flink metrics, re-create the
//!   Mattermost channels, restart the seeded ShadowTraffic runs, redeploy the
//!   baselines, and start the agents again.
//! - `run [--scenario 1|2|3|all]`: execute the timeline
//!   (`director/timeline.yaml`): deploys and fault injections at fixed
//!   offsets.
//! - `status`: progress through the latest run, and the incidents so far.
//! - `incidents`: the latest run's incidents, one per line, for comparing
//!   runs.
//! - `inject <fault>`: start one fault injector now, outside the timeline.
//! - `record`: save the latest run, as the audience saw it, to a file
//!   (`recording`).
//! - `replay start <recording>`: quiet the platform and play a recording
//!   back in its place, with no agents or models (`replay`); `replay
//!   pause|resume|seek|status|stop` steer it. `play` is the player itself,
//!   run by the `replay` Compose service.
//!
//! It runs on the host, beside `docker compose`: from the repo locally, or
//! from `/opt/platform` on the EC2 instance (`scripts/director.sh`).

mod incidents;
mod platform;
mod recording;
mod replay;
mod state;
mod timeline;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use platform::{Endpoints, Platform, step};
use platform_clients::chatops::ChatopsClient;
use recording::{Recording, TABLES};
use state::{Outcome, Run, State, StepRecord, StepStatus};
use timeline::{Action, Selection, Timeline};

#[derive(Debug, Parser)]
#[command(about = "Resets the demo platform and runs the scripted, seeded timeline")]
struct Cli {
    /// The directory with the Compose files.
    #[arg(long, env = "PLATFORM_DIR", default_value = ".", global = true)]
    dir: PathBuf,
    /// The timeline, relative to the platform directory.
    #[arg(long, env = "TIMELINE", default_value = "director/timeline.yaml", global = true)]
    timeline: PathBuf,
    #[command(flatten)]
    endpoints: Endpoints,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Return the platform to its starting state.
    Reset,
    /// Run the timeline, or one scenario's part of it.
    Run {
        #[arg(long, default_value = "all")]
        scenario: Selection,
    },
    /// Show progress through the latest run.
    Status,
    /// List the latest run's incidents, one per line.
    Incidents {
        /// Include each incident's time since the run started.
        #[arg(long)]
        times: bool,
    },
    /// Start a fault injector now (e.g. f2, f3, f5).
    Inject { fault: String },
    /// Save the latest run, as the audience saw it, for `replay`.
    Record {
        /// Where to write it; by default `recordings/<start>-<fleet>.json.gz`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// How much of the run to keep, from its start (default: to its end,
        /// or to now if it's still going).
        #[arg(long, value_parser = humantime::parse_duration)]
        until: Option<Duration>,
    },
    /// Play a recorded run back in place of the live platform.
    Replay {
        #[command(subcommand)]
        command: ReplayCommand,
    },
    /// The player behind `replay start`, run by the `replay` Compose service.
    #[command(hide = true)]
    Play {
        recording: PathBuf,
        #[arg(long, env = "REPLAY_FROM", default_value = "0s", value_parser = humantime::parse_duration)]
        from: Duration,
        #[arg(long, env = "REPLAY_SPEED", default_value_t = 1.0)]
        speed: f64,
        #[arg(long, default_value = "0.0.0.0:9249")]
        metrics_addr: SocketAddr,
        #[arg(long, default_value = "0.0.0.0:8096")]
        control_addr: SocketAddr,
    },
}

#[derive(Debug, Subcommand)]
enum ReplayCommand {
    /// Stop the live platform's agents, data and jobs, empty the channels and
    /// play the recording from `--from`.
    Start {
        recording: PathBuf,
        /// Where in the recording to start (e.g. 11m); everything before is
        /// shown at once.
        #[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
        from: Duration,
        /// 2 plays twice as fast.
        #[arg(long, default_value_t = 1.0)]
        speed: f64,
    },
    Pause,
    Resume,
    /// Jump forward to a moment of the recording (e.g. 15m).
    Seek {
        to: String,
    },
    Status,
    /// Stop the player. `director reset` brings the live platform back.
    Stop,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    // The player and the replay's controls need no platform directory.
    match &cli.command {
        Command::Play { recording, from, speed, metrics_addr, control_addr } => {
            let options = replay::PlayOptions {
                from: *from,
                speed: *speed,
                chatops_url: cli.endpoints.chatops_url.clone(),
                ops_database_url: cli.endpoints.ops_database_url.clone(),
                tickets_database_url: cli.endpoints.tickets_database_url.clone(),
                metrics_addr: *metrics_addr,
                control_addr: *control_addr,
            };
            return replay::play(recording, options).await;
        }
        Command::Replay { command: ReplayCommand::Start { .. } | ReplayCommand::Stop } => {}
        Command::Replay { command } => return replay_control(&cli.endpoints.replay_url, command).await,
        _ => {}
    }
    let dir = cli.dir.canonicalize().with_context(|| format!("no platform directory {}", cli.dir.display()))?;
    let timeline = Timeline::load(&dir.join(&cli.timeline))?;
    let platform = Platform::new(dir, cli.endpoints)?;
    match cli.command {
        Command::Reset => reset(&platform, &timeline).await,
        Command::Run { scenario } => run(&platform, &timeline, scenario).await,
        Command::Status => status(&platform).await,
        Command::Incidents { times } => print_incidents(&platform, times).await,
        Command::Inject { fault } => {
            platform.inject(&fault, timeline.seed).await?;
            println!("started st-fault-{fault}");
            Ok(())
        }
        Command::Record { out, until } => record(&platform, out, until).await,
        Command::Replay { command: ReplayCommand::Start { recording, from, speed } } => {
            replay_start(&platform, &recording, from, speed).await
        }
        Command::Replay { command: ReplayCommand::Stop } => {
            platform.stop_replay().await?;
            println!("Stopped the replay. `director reset` brings the live platform back.");
            Ok(())
        }
        Command::Replay { .. } | Command::Play { .. } => unreachable!("handled above"),
    }
}

async fn reset(platform: &Platform, timeline: &Timeline) -> anyhow::Result<()> {
    println!("Resetting the platform (seed {})", timeline.seed);

    step("stopping the fault injectors, any replay, the generators, the fleet relay and the agents");
    platform.stop_faults().await?;
    platform.stop_replay().await?;
    platform.compose(&[], &["stop", "st-inputs", "st-fleet", "fleet-exporter", "triage", "operator"]).await?;

    let cancelled = platform.cancel_all_jobs().await?;
    step(&format!("cancelled {cancelled} Flink jobs"));
    // The collector keeps polling a stopped run for 15 s; let it finish, so
    // nothing from before the reset lands after it.
    tokio::time::sleep(Duration::from_secs(20)).await;

    // The jobs' groups are named after them; the agents' after the agents.
    let mut groups = platform.catalogue_jobs().await?;
    groups.extend(["triage".to_string(), "operator".to_string()]);
    for group in &groups {
        if let Err(err) = platform.skip_to_latest(group).await {
            tracing::warn!(error = format!("{err:#}"), group, "skipping the consumer group to latest");
        }
    }
    step(&format!("skipped {} consumer groups to the latest offsets", groups.len()));

    platform.truncate().await?;
    step("emptied the ops DB's history and the tickets");

    platform.forget_metrics().await?;
    platform.compose(&[], &["up", "-d", "--wait", "--force-recreate", "prometheus", "alertmanager"]).await?;
    step("forgot the Flink metrics; restarted Prometheus and Alertmanager");

    platform.reset_channels().await?;
    step("archived and re-created the Mattermost channels");

    restart_generators(platform, timeline).await?;

    platform.baseline().await?;
    step("deployed every job's baseline version");

    platform.compose(&[], &["up", "-d", "--wait", "triage", "operator"]).await?;
    step("started the triage and operator agents");

    let mut state = State::load(&platform.dir);
    state.reset_at = Some(Utc::now());
    state.run = None;
    state.save(&platform.dir)?;
    println!("Reset done. Next: director run --scenario all");
    Ok(())
}

async fn run(platform: &Platform, timeline: &Timeline, selection: Selection) -> anyhow::Result<()> {
    let plan = timeline.plan(selection)?;
    let mut state = State::load(&platform.dir);
    if state.reset_at.is_none() {
        println!("Warning: no reset since the last run; incidents may carry over (run `director reset` first)");
    }
    // The fleet's problems and the input data follow their seeds from when
    // they start, so start them with the run: then they line up with the
    // timeline's offsets however long the reset took.
    restart_generators(platform, timeline).await?;
    let started_at = Utc::now();
    state.reset_at = None;
    state.run = Some(Run {
        scenario: selection.to_string(),
        seed: timeline.seed,
        started_at,
        ends_at: started_at + chrono::Duration::from_std(plan.end)?,
        outcome: Outcome::Running,
        steps: plan
            .steps
            .iter()
            .map(|s| StepRecord {
                at_secs: s.at.as_secs(),
                scenario: s.scenario,
                action: s.action.describe(),
                note: s.note.clone(),
                status: StepStatus::Pending,
                done_at: None,
                detail: None,
            })
            .collect(),
    });
    state.save(&platform.dir)?;
    let title = match selection {
        Selection::All => "the whole timeline".to_string(),
        Selection::Scenario(n) => format!("scenario {n}: {}", timeline.scenarios[&n].title),
    };
    println!("Running {title}: {} steps over {}", plan.steps.len(), clock(plan.end));

    let mut failed = false;
    for (i, planned) in plan.steps.iter().enumerate() {
        wait_until(started_at + planned.at).await;
        let result = match &planned.action {
            Action::Deploy { job, version, args } => platform
                .deploy(job, version, args)
                .await
                .map(|d| format!("flink job {}", d["flink_job_id"].as_str().unwrap_or("?"))),
            Action::Fault(name) => {
                platform.inject(name, timeline.seed).await.map(|()| format!("st-fault-{name} started"))
            }
        };
        let record = &mut state.run.as_mut().expect("run").steps[i];
        record.done_at = Some(Utc::now());
        match result {
            Ok(detail) => {
                record.status = StepStatus::Done;
                println!("[{}] {} — {} ({detail})", clock(planned.at), record.action, record.note);
                record.detail = Some(detail);
            }
            Err(err) => {
                failed = true;
                record.status = StepStatus::Failed;
                record.detail = Some(format!("{err:#}"));
                println!("[{}] {} FAILED: {err:#}", clock(planned.at), record.action);
            }
        }
        state.save(&platform.dir)?;
    }
    wait_until(started_at + plan.end).await;
    state.run.as_mut().expect("run").outcome = if failed { Outcome::Failed } else { Outcome::Finished };
    state.save(&platform.dir)?;
    println!("[{}] end of the run", clock(plan.end));
    print_incidents(platform, true).await?;
    anyhow::ensure!(!failed, "some steps failed");
    Ok(())
}

/// Restarts the fleet (and its relay) and the input data with the timeline's
/// seeds.
async fn restart_generators(platform: &Platform, timeline: &Timeline) -> anyhow::Result<()> {
    platform
        .compose(
            &platform::seed_env(timeline.seed),
            &["up", "-d", "--wait", "--force-recreate", "fleet-exporter", "st-fleet", "st-inputs"],
        )
        .await?;
    step("restarted the fleet and the input data with the timeline's seeds");
    Ok(())
}

/// Waits for a wall-clock time. Monotonic timers pause while a laptop
/// sleeps (and Docker's VM with it), which would push every later step back
/// by the time asleep; after a sleep, overdue steps run at once instead.
async fn wait_until(deadline: chrono::DateTime<Utc>) {
    while let Ok(left) = (deadline - Utc::now()).to_std() {
        tokio::time::sleep(left.min(Duration::from_secs(5))).await;
    }
}

/// Saves the latest run: the chatops-bot's transcript, the rows Grafana and
/// the tickets UI read, and the real jobs' metrics.
async fn record(platform: &Platform, out: Option<PathBuf>, until: Option<Duration>) -> anyhow::Result<()> {
    let state = State::load(&platform.dir);
    let run = state.run.context("no run to record; `director run` first")?;
    let now = Utc::now();
    let ends_at = match until {
        Some(until) => run.started_at + chrono::Duration::from_std(until)?,
        None => run.ends_at,
    }
    .min(now);
    if ends_at < run.ends_at {
        println!(
            "Recording up to {} of the run's {}",
            clock_between(run.started_at, ends_at),
            clock_between(run.started_at, run.ends_at)
        );
    }
    let endpoints = &platform.endpoints;
    let transcript: Vec<_> = ChatopsClient::new(&endpoints.chatops_url)
        .transcript()
        .await
        .context("reading the chatops-bot's transcript")?
        .into_iter()
        .filter(|e| e.ts >= run.started_at && e.ts <= ends_at)
        .collect();
    anyhow::ensure!(
        !transcript.is_empty(),
        "the chatops-bot's transcript has nothing from this run (was it restarted since the reset?)"
    );
    let mut tables = Vec::new();
    for (database, table, time_column) in TABLES {
        let url = match database {
            recording::Database::Ops => &endpoints.ops_database_url,
            recording::Database::Tickets => &endpoints.tickets_database_url,
        };
        tables.push(recording::read_table(url, database, table, time_column, run.started_at, ends_at).await?);
    }
    let jobs = platform.catalogue_jobs().await?;
    let series = recording::read_series(&endpoints.prometheus_url, &jobs, run.started_at, ends_at).await?;
    let fleet = platform::fleet_size(&platform.roster_path().await?)?;
    let recording = Recording {
        format: recording::FORMAT,
        recorded_at: now,
        fleet: format!("{fleet}-job fleet"),
        seed: run.seed,
        scenario: run.scenario.clone(),
        started_at: run.started_at,
        ends_at,
        steps: run.steps.clone(),
        transcript,
        tables,
        series,
    };
    let out = out.unwrap_or_else(|| {
        platform.dir.join("recordings").join(format!("{}-{fleet}-jobs.json.gz", run.started_at.format("%Y%m%d-%H%M")))
    });
    recording.save(&out)?;
    let size = std::fs::metadata(&out)?.len();
    println!("Recorded the {}", recording.describe());
    println!("Saved {} ({:.1} MB)", out.display(), size as f64 / 1e6);
    Ok(())
}

/// Quiets the live platform (no agents, data, jobs or alerts; empty
/// channels, history and tickets) and starts the `replay` service.
async fn replay_start(platform: &Platform, path: &Path, from: Duration, speed: f64) -> anyhow::Result<()> {
    let recording = Recording::load(path)?;
    anyhow::ensure!(speed > 0.0, "the speed must be positive");
    anyhow::ensure!(
        from.as_secs_f64() < recording.length(),
        "--from {} is past the recording's end ({})",
        clock(from),
        clock(Duration::from_secs_f64(recording.length()))
    );
    println!("Replaying the {}", recording.describe());

    step("stopping the fault injectors, any replay, the generators, the fleet relay, the agents and Alertmanager");
    platform.stop_faults().await?;
    platform.stop_replay().await?;
    platform
        .compose(&[], &["stop", "st-inputs", "st-fleet", "fleet-exporter", "triage", "operator", "alertmanager"])
        .await?;
    let cancelled = platform.cancel_all_jobs().await?;
    step(&format!("cancelled {cancelled} Flink jobs"));
    platform.forget_metrics().await?;
    step("forgot the Flink metrics");
    platform.truncate().await?;
    step("emptied the ops DB's history and the tickets");
    platform.reset_channels().await?;
    step("archived and re-created the Mattermost channels");

    // The service reads it from the platform directory's .director.
    let staged = platform.dir.join(".director").join("replay.json.gz");
    std::fs::create_dir_all(staged.parent().expect("a parent"))?;
    if path.canonicalize()? != staged.canonicalize().unwrap_or_default() {
        std::fs::copy(path, &staged)?;
    }
    platform
        .compose(
            &[("REPLAY_FROM", format!("{}s", from.as_secs())), ("REPLAY_SPEED", speed.to_string())],
            &["--profile", "replay", "up", "-d", "--force-recreate", "--no-deps", "--wait", "replay"],
        )
        .await?;
    step(&format!("started the replay at {} ({speed}×)", clock(from)));

    let mut state = State::load(&platform.dir);
    state.reset_at = None;
    state.run = None;
    state.save(&platform.dir)?;
    println!(
        "Replaying. Steer it with `director replay pause|resume|seek <t>|status|stop`; `director reset` goes live again."
    );
    Ok(())
}

/// `replay pause|resume|seek|status`, through the player's control API.
async fn replay_control(url: &str, command: &ReplayCommand) -> anyhow::Result<()> {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
    let request = match command {
        ReplayCommand::Pause => http.post(format!("{url}/pause")),
        ReplayCommand::Resume => http.post(format!("{url}/resume")),
        ReplayCommand::Seek { to } => http.post(format!("{url}/seek")).query(&[("to", to)]),
        ReplayCommand::Status => http.get(format!("{url}/status")),
        ReplayCommand::Start { .. } | ReplayCommand::Stop => unreachable!("not a control call"),
    };
    let body: serde_json::Value = request
        .send()
        .await
        .context("reaching the replay (is one running? `director replay start <recording>`)")?
        .error_for_status()?
        .json()
        .await?;
    if let Some(error) = body["error"].as_str() {
        anyhow::bail!("{error}");
    }
    let status: replay::Status = serde_json::from_value(body)?;
    let at = |secs: f64| clock(Duration::from_secs_f64(secs.max(0.0)));
    let state = match (status.paused, status.finished) {
        (_, true) => "finished",
        (true, false) => "paused",
        (false, false) => "playing",
    };
    println!(
        "Replay {state} at {} of {}, {}×: {} posts, {} edits, {} rows so far",
        at(status.offset),
        at(status.end),
        status.speed,
        status.posts,
        status.edits,
        status.rows
    );
    if let Some((offset, step)) = status.next_step {
        println!("  next: [{}] {step}", at(offset));
    }
    println!("  {}", status.recording);
    Ok(())
}

/// `mm:ss` between two times.
fn clock_between(from: chrono::DateTime<Utc>, to: chrono::DateTime<Utc>) -> String {
    clock((to - from).to_std().unwrap_or_default())
}

/// `mm:ss`.
fn clock(d: Duration) -> String {
    format!("{:02}:{:02}", d.as_secs() / 60, d.as_secs() % 60)
}

async fn incidents(platform: &Platform, run: &Run) -> anyhow::Result<Vec<incidents::Incident>> {
    let jobs = platform.catalogue_jobs().await?;
    let fleet = platform::genuine_fleet_jobs(&platform.roster_path().await?)?;
    incidents::since(&platform.endpoints.ops_database_url, run.started_at, &jobs, &fleet).await
}

async fn print_incidents(platform: &Platform, times: bool) -> anyhow::Result<()> {
    let state = State::load(&platform.dir);
    let run = state.run.context("no run yet")?;
    for incident in incidents(platform, &run).await? {
        if times {
            let offset = (incident.ts - run.started_at).to_std().unwrap_or_default();
            println!("{}  {}", clock(offset), incident.describe());
        } else {
            println!("{}", incident.signature());
        }
    }
    Ok(())
}

async fn status(platform: &Platform) -> anyhow::Result<()> {
    let state = State::load(&platform.dir);
    let Some(run) = state.run.clone() else {
        match state.reset_at {
            Some(at) => println!("Reset at {at}; no run since."),
            None => println!("No reset or run yet."),
        }
        return Ok(());
    };
    let elapsed = (Utc::now() - run.started_at).to_std().unwrap_or_default();
    let total = (run.ends_at - run.started_at).to_std().unwrap_or_default();
    let outcome = match run.outcome {
        Outcome::Running if Utc::now() > run.ends_at + chrono::Duration::minutes(1) => {
            "overdue: the director exited or was stopped before the end"
        }
        Outcome::Running => "running",
        Outcome::Finished => "finished",
        Outcome::Failed => "finished with failed steps",
    };
    println!(
        "Run of scenario {} (seed {}), {outcome}: {} of {}",
        run.scenario,
        run.seed,
        clock(elapsed.min(total)),
        clock(total)
    );
    for step in &run.steps {
        let mark = match step.status {
            StepStatus::Done => "✓",
            StepStatus::Failed => "✗",
            StepStatus::Pending => "·",
        };
        println!("  {mark} [{}] {} — {}", clock(Duration::from_secs(step.at_secs)), step.action, step.note);
        if let (StepStatus::Failed, Some(detail)) = (step.status, &step.detail) {
            println!("      {detail}");
        }
    }
    println!("Incidents so far:");
    match incidents(platform, &run).await {
        Ok(list) if list.is_empty() => println!("  none"),
        Ok(list) => {
            for incident in list {
                let offset = (incident.ts - run.started_at).to_std().unwrap_or_default();
                println!("  [{}] {}", clock(offset), incident.describe());
            }
        }
        Err(err) => println!("  unavailable: {err:#}"),
    }
    Ok(())
}
