//! Consumes the `platform.*` topics into the ops DB, and enforces the log
//! retention window. Metrics go to Prometheus, not the ops DB.
//!
//! Offsets are stored only after a write succeeds, so a DB outage pauses the
//! sink rather than losing events. An event the model can't decode is logged
//! and skipped.

use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use platform_clients::ops_db::OpsDb;
use platform_model::topics;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    message::BorrowedMessage,
};
use serde::de::DeserializeOwned;
use tracing::{info, warn};

const TOPICS: [&str; 8] = [
    topics::JOBS,
    topics::DEPLOYMENTS,
    topics::JOB_STATUS,
    topics::CHECKPOINTS,
    topics::LOGS,
    topics::ALERTS,
    topics::SIGNALS,
    topics::ACTIONS,
];

pub async fn run(bootstrap_servers: String, db: OpsDb) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "collector-ops-db")
        .set("client.id", "collector-ops-db")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .create()
        .context("creating the Kafka consumer")?;
    consumer.subscribe(&TOPICS).context("subscribing to the platform topics")?;
    info!(topics = ?TOPICS, "ops DB sink started");

    loop {
        let message = match consumer.recv().await {
            Ok(message) => message,
            Err(err) => {
                warn!(%err, "Kafka consumer error");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let mut backoff = Duration::from_millis(500);
        loop {
            match store(&db, &message).await {
                Ok(()) => break,
                Err(err) if err.is::<platform_clients::ops_db::Error>() => {
                    warn!(%err, topic = message.topic(), "ops DB write failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
                Err(err) => {
                    warn!(%err, topic = message.topic(), offset = message.offset(), "skipping an undecodable event");
                    break;
                }
            }
        }
        consumer.store_offset_from_message(&message).context("storing the consumer offset")?;
    }
}

fn decode<T: DeserializeOwned>(payload: &[u8]) -> anyhow::Result<T> {
    Ok(serde_json::from_slice(payload)?)
}

async fn store(db: &OpsDb, message: &BorrowedMessage<'_>) -> anyhow::Result<()> {
    let Some(payload) = message.payload() else {
        // A tombstone: only the compacted job registry uses them.
        if message.topic() == topics::JOBS
            && let Some(key) = message.key()
        {
            db.delete_job(&String::from_utf8_lossy(key)).await?;
        }
        return Ok(());
    };
    match message.topic() {
        topics::JOBS => db.upsert_job(&decode(payload)?).await?,
        topics::DEPLOYMENTS => db.insert_deployment(&decode(payload)?).await?,
        topics::JOB_STATUS => db.insert_job_status(&decode(payload)?).await?,
        topics::CHECKPOINTS => db.insert_checkpoint(&decode(payload)?).await?,
        topics::LOGS => db.insert_log(&decode(payload)?).await?,
        topics::ALERTS => db.insert_alert(&decode(payload)?).await?,
        topics::SIGNALS => db.insert_signal(&decode(payload)?).await?,
        topics::ACTIONS => db.insert_action(&decode(payload)?).await?,
        other => warn!(topic = other, "unexpected topic"),
    }
    Ok(())
}

/// Deletes logs older than the retention window, once a minute.
pub async fn retain_logs(db: OpsDb, retention: Duration) -> anyhow::Result<()> {
    let retention = chrono::Duration::from_std(retention)?;
    let mut ticker = tokio::time::interval(Duration::from_secs(60));
    loop {
        ticker.tick().await;
        match db.delete_logs_before(Utc::now() - retention).await {
            Ok(0) => {}
            Ok(deleted) => info!(deleted, "deleted logs past the retention window"),
            Err(err) => warn!(%err, "deleting old logs"),
        }
    }
}
