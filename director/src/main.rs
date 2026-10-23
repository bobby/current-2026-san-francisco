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
//!
//! It runs on the host, beside `docker compose`: from the repo locally, or
//! from `/opt/platform` on the EC2 instance (`scripts/director.sh`).

mod incidents;
mod platform;
mod state;
mod timeline;

use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use platform::{Endpoints, Platform, step};
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
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
    }
}

async fn reset(platform: &Platform, timeline: &Timeline) -> anyhow::Result<()> {
    println!("Resetting the platform (seed {})", timeline.seed);

    step("stopping the fault injectors, the generators, the fleet relay and the agents");
    platform.stop_faults().await?;
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
