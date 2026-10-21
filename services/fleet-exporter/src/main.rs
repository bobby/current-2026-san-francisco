//! The fleet-exporter (docs/design.md §5.2): the simulated fleet's relay.
//!
//! It consumes `fleet.telemetry`, ShadowTraffic's per-job lifecycle stream,
//! and for each simulated job:
//! - publishes the `platform.*` events a real job would produce (registry,
//!   deployments, status, checkpoints, logs), with consistent run IDs,
//!   counters and versions (see `sim`);
//! - serves Prometheus metrics on `:9249/metrics` with the names and labels
//!   of Flink's own reporter, so Prometheus scrapes it as part of the `flink`
//!   job and queries can't tell simulated jobs from real ones.
//!
//! It starts from the latest offsets of its consumer group, so a restart
//! continues the fleet rather than replaying it.

mod exposition;
mod sim;

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use axum::{Router, extract::State, routing::get};
use chrono::Utc;
use clap::Parser;
use platform_clients::kafka::Publisher;
use platform_model::{fleet, topics};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use sim::{Fleet, Output};
use tracing::{info, warn};

#[derive(Debug, Clone, Parser)]
#[command(about = "Relays the simulated fleet into platform.* events and Prometheus metrics")]
struct Config {
    #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS", default_value = "kafka:9092")]
    kafka_bootstrap_servers: String,
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:9249")]
    listen_addr: SocketAddr,
    /// Jobs not heard from for this long drop out of /metrics.
    #[arg(long, env = "STALE_AFTER", default_value = "5m", value_parser = humantime::parse_duration)]
    stale_after: Duration,
}

type SharedFleet = Arc<Mutex<Fleet>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = Config::parse();
    let fleet: SharedFleet = Default::default();
    let publisher = Publisher::new(&config.kafka_bootstrap_servers, "fleet-exporter")?;

    let stale_after = chrono::Duration::from_std(config.stale_after)?;
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/metrics",
            get(move |State(fleet): State<SharedFleet>| async move {
                let now = Utc::now();
                let fleet = fleet.lock().unwrap();
                exposition::render(fleet.jobs.values(), now, now - stale_after)
            }),
        )
        .with_state(fleet.clone());
    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %config.listen_addr, "serving /metrics");
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    tokio::select! {
        result = relay(&config.kafka_bootstrap_servers, fleet, publisher) => result,
        result = server => result?.context("HTTP server"),
    }
}

async fn relay(bootstrap_servers: &str, fleet: SharedFleet, publisher: Publisher) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "fleet-exporter")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
        .context("creating the Kafka consumer")?;
    consumer.subscribe(&[fleet::TOPIC])?;
    info!(topic = fleet::TOPIC, "relaying the simulated fleet");
    loop {
        let message = match consumer.recv().await {
            Ok(message) => message,
            Err(err) => {
                warn!(%err, "Kafka consumer error");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let Some(payload) = message.payload() else { continue };
        let event: fleet::FleetEvent = match serde_json::from_slice(payload) {
            Ok(event) => event,
            Err(err) => {
                warn!(%err, offset = message.offset(), "skipping an undecodable fleet event");
                continue;
            }
        };
        let outputs = fleet.lock().unwrap().apply(event, Utc::now());
        for output in outputs {
            let result = match &output {
                Output::Job(e) => publisher.publish(topics::JOBS, &e.job_name, e).await,
                Output::Deployment(e) => publisher.publish(topics::DEPLOYMENTS, &e.job_name, e).await,
                Output::Status(e) => publisher.publish(topics::JOB_STATUS, &e.job_name, e).await,
                Output::Checkpoint(e) => publisher.publish(topics::CHECKPOINTS, &e.job_name, e).await,
                Output::Log(e) => publisher.publish(topics::LOGS, &e.job_name, e).await,
            };
            if let Err(err) = result {
                warn!(%err, "publishing a fleet event");
            }
        }
    }
}
