//! What the director drives: Docker Compose (the ShadowTraffic runs and
//! service restarts), cicd-sim, the chatops-bot, Flink, Prometheus and the
//! ops and tickets databases. Everything is reached the way an operator on
//! the host would, through localhost ports and `docker compose`, so the
//! director runs the same on the laptop and on the EC2 instance.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context;
use platform_clients::flink::FlinkClient;
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};
use tracing::{info, warn};

#[derive(Debug, Clone, clap::Args)]
pub struct Endpoints {
    #[arg(long, env = "CICD_URL", default_value = "http://localhost:8091")]
    pub cicd_url: String,
    #[arg(long, env = "CHATOPS_URL", default_value = "http://localhost:8092")]
    pub chatops_url: String,
    #[arg(long, env = "FLINK_URL", default_value = "http://localhost:8081")]
    pub flink_url: String,
    #[arg(long, env = "PROMETHEUS_URL", default_value = "http://localhost:9090")]
    pub prometheus_url: String,
    #[arg(long, env = "DIRECTOR_OPS_DATABASE_URL", default_value = "postgres://ops:ops@localhost:5432/ops")]
    pub ops_database_url: String,
    #[arg(
        long,
        env = "DIRECTOR_TICKETS_DATABASE_URL",
        default_value = "postgres://tickets:tickets@localhost:5432/tickets"
    )]
    pub tickets_database_url: String,
}

/// The fault injectors, as Compose services in the `faults` profile.
pub const FAULT_SERVICES: [&str; 3] = ["st-fault-f2", "st-fault-f3", "st-fault-f5"];

/// The ops DB's history tables. The job registry (`jobs`) is current state,
/// not history, and the simulated fleet only re-registers on deploys, so a
/// reset keeps it.
const OPS_HISTORY: &str = "deployments, job_status, checkpoints, logs, alerts, signals, actions";

pub struct Platform {
    /// The directory with the Compose files (the repo, or `/opt/platform`).
    pub dir: PathBuf,
    pub endpoints: Endpoints,
    http: reqwest::Client,
    flink: FlinkClient,
}

/// The seeds for each ShadowTraffic run, from the timeline's seed.
pub fn seed_env(seed: u64) -> Vec<(&'static str, String)> {
    vec![
        ("ST_INPUTS_SEED", seed.to_string()),
        ("ST_FLEET_SEED", (seed + 1).to_string()),
        ("ST_FAULT_SEED", (seed + 2).to_string()),
    ]
}

impl Platform {
    pub fn new(dir: PathBuf, endpoints: Endpoints) -> anyhow::Result<Self> {
        let flink = FlinkClient::new(&endpoints.flink_url);
        let http = reqwest::Client::builder().timeout(Duration::from_secs(600)).build()?;
        Ok(Self { dir, endpoints, http, flink })
    }

    /// Runs `docker compose <args>` in the platform directory.
    pub async fn compose(&self, env: &[(&str, String)], args: &[&str]) -> anyhow::Result<String> {
        let output = tokio::process::Command::new("docker")
            .arg("compose")
            .args(args)
            .current_dir(&self.dir)
            .envs(env.iter().map(|(k, v)| (*k, v.as_str())))
            .output()
            .await
            .context("running docker compose")?;
        anyhow::ensure!(
            output.status.success(),
            "docker compose {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Starts a fault injector afresh. It stops on its own.
    pub async fn inject(&self, fault: &str, seed: u64) -> anyhow::Result<()> {
        let service = format!("st-fault-{fault}");
        self.compose(&seed_env(seed), &["--profile", "faults", "up", "-d", "--force-recreate", "--no-deps", &service])
            .await?;
        Ok(())
    }

    pub async fn stop_faults(&self) -> anyhow::Result<()> {
        let mut args = vec!["--profile", "faults", "rm", "--stop", "--force"];
        args.extend(FAULT_SERVICES);
        self.compose(&[], &args).await.map(drop)
    }

    pub async fn catalogue_jobs(&self) -> anyhow::Result<Vec<String>> {
        let catalogue: Value = self.get_json(&format!("{}/catalogue", self.endpoints.cicd_url)).await?;
        let jobs = catalogue["jobs"].as_object().context("the catalogue has no jobs")?;
        Ok(jobs.keys().cloned().collect())
    }

    /// A deploy through cicd-sim, as the director.
    pub async fn deploy(&self, job: &str, version: &str, args: &[String]) -> anyhow::Result<Value> {
        let body = json!({"job": job, "version": version, "args": args, "triggered_by": "director"});
        let response = self.http.post(format!("{}/deploy", self.endpoints.cicd_url)).json(&body).send().await?;
        let status = response.status();
        let deployment: Value = response.json().await?;
        anyhow::ensure!(status.is_success(), "cicd-sim answered {status}: {deployment}");
        Ok(deployment)
    }

    pub async fn baseline(&self) -> anyhow::Result<()> {
        let response = self
            .http
            .post(format!("{}/baseline", self.endpoints.cicd_url))
            .json(&json!({"triggered_by": "director"}))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::ensure!(status.is_success(), "baseline failed ({status}): {body}");
        Ok(())
    }

    /// Cancels every running Flink job and waits until they've all stopped.
    pub async fn cancel_all_jobs(&self) -> anyhow::Result<usize> {
        let active: Vec<_> = self.flink.jobs().await?.into_iter().filter(|j| !j.state.is_terminal()).collect();
        for job in &active {
            if let Err(err) = self.flink.cancel(&job.jid).await {
                warn!(%err, job = job.name, "cancelling");
            }
        }
        for _ in 0..60 {
            if self.flink.jobs().await?.iter().all(|j| j.state.is_terminal()) {
                return Ok(active.len());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        anyhow::bail!("Flink jobs still running a minute after cancelling them")
    }

    /// Moves a consumer group (a job's is named after it) to the end of its
    /// topics, so the next run starts with no backlog and no leftover fault
    /// records or alerts.
    pub async fn skip_to_latest(&self, group: &str) -> anyhow::Result<()> {
        self.compose(
            &[],
            &[
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--group",
                group,
                "--reset-offsets",
                "--to-latest",
                "--all-topics",
                "--execute",
            ],
        )
        .await
        .map(drop)
    }

    /// Empties the ops DB's history and the tickets.
    pub async fn truncate(&self) -> anyhow::Result<()> {
        let mut ops =
            PgConnection::connect(&self.endpoints.ops_database_url).await.context("connecting to the ops DB")?;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("TRUNCATE {OPS_HISTORY}"))).execute(&mut ops).await?;
        let mut tickets = PgConnection::connect(&self.endpoints.tickets_database_url)
            .await
            .context("connecting to the tickets DB")?;
        sqlx::raw_sql("TRUNCATE incident, journal RESTART IDENTITY").execute(&mut tickets).await?;
        Ok(())
    }

    /// Deletes Prometheus's Flink series, so the naive rules don't fire on
    /// what happened before the reset.
    pub async fn forget_metrics(&self) -> anyhow::Result<()> {
        let base = &self.endpoints.prometheus_url;
        self.http
            .post(format!("{base}/api/v1/admin/tsdb/delete_series"))
            .query(&[("match[]", r#"{__name__=~"flink_.*"}"#)])
            .send()
            .await?
            .error_for_status()
            .context("deleting series (is Prometheus's admin API on?)")?;
        self.http.post(format!("{base}/api/v1/admin/tsdb/clean_tombstones")).send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn reset_channels(&self) -> anyhow::Result<()> {
        self.http
            .post(format!("{}/admin/reset-channels", self.endpoints.chatops_url))
            .send()
            .await?
            .error_for_status()
            .context("resetting the Mattermost channels")?;
        Ok(())
    }

    /// The simulated fleet's roster, as mounted into `st-fleet` (local or
    /// cloud, depending on the Compose files in use).
    pub async fn roster_path(&self) -> anyhow::Result<PathBuf> {
        let config: Value = serde_json::from_str(&self.compose(&[], &["config", "--format", "json"]).await?)?;
        let volumes = config["services"]["st-fleet"]["volumes"].as_array().context("st-fleet has no volumes")?;
        let source = volumes
            .iter()
            .find(|v| v["target"] == "/fleet/roster.json")
            .and_then(|v| v["source"].as_str())
            .context("st-fleet mounts no roster")?;
        Ok(PathBuf::from(source))
    }

    async fn get_json(&self, url: &str) -> anyhow::Result<Value> {
        Ok(self.http.get(url).send().await?.error_for_status()?.json().await?)
    }
}

/// The fleet jobs with genuine problems (crash loop, stalled consumer, state
/// growth), from a roster file.
pub fn genuine_fleet_jobs(roster: &Path) -> anyhow::Result<Vec<String>> {
    let jobs: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(roster)?)?;
    Ok(jobs
        .iter()
        .filter(|j| matches!(j["profile"].as_str(), Some("crash_loop" | "stalled" | "state_growth")))
        .filter_map(|j| j["job_name"].as_str().map(str::to_string))
        .collect())
}

/// Logs a reset step.
pub fn step(message: &str) {
    info!("{message}");
    println!("  • {message}");
}
