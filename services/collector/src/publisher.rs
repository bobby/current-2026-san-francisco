//! Publishes events to the `platform.*` topics, keyed by job name.

use std::time::Duration;

use anyhow::Context;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde::Serialize;

#[derive(Clone)]
pub struct Publisher {
    producer: FutureProducer,
}

impl Publisher {
    pub fn new(bootstrap_servers: &str) -> anyhow::Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("client.id", "collector")
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("linger.ms", "20")
            .create()
            .context("creating the Kafka producer")?;
        Ok(Self { producer })
    }

    pub async fn publish<T: Serialize>(&self, topic: &str, key: &str, event: &T) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(event)?;
        self.producer
            .send(FutureRecord::to(topic).key(key).payload(&payload), Duration::from_secs(10))
            .await
            .map_err(|(err, _)| err)
            .with_context(|| format!("publishing to {topic}"))?;
        Ok(())
    }
}
