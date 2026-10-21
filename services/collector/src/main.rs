//! The collector (docs/design.md §5): the bridge between the real platform
//! and the telemetry plane.
//!
//! - Polls the Flink REST API and publishes `platform.job-status` and
//!   `platform.checkpoints`.
//! - Receives Alertmanager webhooks (`POST /alertmanager`) and publishes
//!   `platform.alerts`.
//! - Receives Flink's JSON logs from Vector (`POST /logs`), adds the job name
//!   and publishes `platform.logs`.
//! - Consumes every `platform.*` topic except metrics into the ops DB, and
//!   deletes logs older than the retention window.

mod http;
mod jobs;
mod poller;
mod publisher;
mod sink;

use std::{net::SocketAddr, time::Duration};

use anyhow::Context;
use clap::Parser;
use platform_clients::{flink::FlinkClient, ops_db::OpsDb};
use tokio::task::JoinSet;
use tracing::{info, warn};

#[derive(Debug, Clone, Parser)]
#[command(about = "Turns Flink activity into platform.* events and fills the ops DB")]
pub struct Config {
    #[arg(long, env = "FLINK_URL", default_value = "http://jobmanager:8081")]
    pub flink_url: String,
    #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS", default_value = "kafka:9092")]
    pub kafka_bootstrap_servers: String,
    #[arg(long, env = "OPS_DATABASE_URL", default_value = "postgres://ops:ops@postgres:5432/ops")]
    pub ops_database_url: String,
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8090")]
    pub listen_addr: SocketAddr,
    /// How often to poll the Flink REST API.
    #[arg(long, env = "POLL_INTERVAL", default_value = "2s", value_parser = humantime::parse_duration)]
    pub poll_interval: Duration,
    /// How long the ops DB keeps logs.
    #[arg(long, env = "LOG_RETENTION", default_value = "24h", value_parser = humantime::parse_duration)]
    pub log_retention: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();
    let config = Config::parse();

    let db = connect_ops_db(&config.ops_database_url).await?;
    let publisher = publisher::Publisher::new(&config.kafka_bootstrap_servers)?;
    let flink = FlinkClient::new(&config.flink_url);
    let jobs = jobs::JobNames::new(flink.clone(), db.clone());

    let mut tasks = JoinSet::new();
    tasks.spawn(poller::run(flink, publisher.clone(), jobs.clone(), config.poll_interval));
    tasks.spawn(http::serve(config.listen_addr, publisher, jobs));
    tasks.spawn(sink::run(config.kafka_bootstrap_servers.clone(), db.clone()));
    tasks.spawn(sink::retain_logs(db, config.log_retention));
    info!(
        flink_url = config.flink_url,
        kafka = config.kafka_bootstrap_servers,
        poll_interval = ?config.poll_interval,
        log_retention = ?config.log_retention,
        "collector started"
    );

    tokio::select! {
        Some(result) = tasks.join_next() => {
            result.context("collector task panicked")?.context("collector task failed")?;
            anyhow::bail!("a collector task stopped unexpectedly");
        }
        _ = shutdown_signal() => info!("shutting down"),
    }
    Ok(())
}

/// Connects to the ops DB and applies migrations, retrying while Postgres
/// starts up.
async fn connect_ops_db(url: &str) -> anyhow::Result<OpsDb> {
    let mut attempt = 0;
    loop {
        match OpsDb::connect(url).await {
            Ok(db) => {
                db.migrate().await.context("migrating the ops DB")?;
                return Ok(db);
            }
            Err(err) if attempt < 30 => {
                attempt += 1;
                warn!(%err, attempt, "ops DB not ready; retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(err) => return Err(err).context("connecting to the ops DB"),
        }
    }
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
