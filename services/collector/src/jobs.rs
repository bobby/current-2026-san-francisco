//! Resolves the platform job name behind a Flink job ID or a Prometheus
//! `job_name` label.
//!
//! Flink's logs carry only the job ID, and its Prometheus reporter rewrites
//! label values (`payments-enrichment` becomes `payments_enrichment`). Real
//! jobs are resolved from the Flink REST API; simulated fleet jobs from the
//! ops DB's job registry.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use platform_clients::{flink::FlinkClient, ops_db::OpsDb};
use tracing::warn;

#[derive(Clone)]
pub struct JobNames {
    flink: FlinkClient,
    db: OpsDb,
    /// Flink job ID → job name.
    by_id: Arc<RwLock<HashMap<String, String>>>,
}

/// Rewrites a label value as Flink's Prometheus reporter does: every
/// character outside `[A-Za-z0-9:_]` becomes `_`.
pub fn prometheus_label(value: &str) -> String {
    value.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' }).collect()
}

impl JobNames {
    pub fn new(flink: FlinkClient, db: OpsDb) -> Self {
        Self { flink, db, by_id: Default::default() }
    }

    /// Records the name of a job the poller has seen.
    pub fn insert(&self, flink_job_id: &str, job_name: &str) {
        self.by_id.write().unwrap().insert(flink_job_id.to_string(), job_name.to_string());
    }

    fn cached(&self, flink_job_id: &str) -> Option<String> {
        self.by_id.read().unwrap().get(flink_job_id).cloned()
    }

    /// The name of a Flink job, asking Flink if it's not cached (a job can
    /// log before the poller first sees it).
    pub async fn by_flink_id(&self, flink_job_id: &str) -> Option<String> {
        if let Some(name) = self.cached(flink_job_id) {
            return Some(name);
        }
        match self.flink.jobs().await {
            Ok(jobs) => jobs.iter().for_each(|job| self.insert(&job.jid, &job.name)),
            Err(err) => warn!(%err, "listing Flink jobs to resolve a job ID"),
        }
        self.cached(flink_job_id)
    }

    /// The platform job name for an alert's labels: from its `job_id` label
    /// (a real Flink job), or by matching the rewritten `job_name` label
    /// against known names. Falls back to the label as given.
    pub async fn for_alert_labels(&self, labels: &HashMap<String, String>) -> Option<String> {
        if let Some(id) = labels.get("job_id")
            && let Some(name) = self.by_flink_id(id).await
        {
            return Some(name);
        }
        let label = labels.get("job_name")?;
        let known = self.by_id.read().unwrap().values().find(|name| prometheus_label(name) == *label).cloned();
        if known.is_some() {
            return known;
        }
        match self.db.job_names().await {
            Ok(names) => {
                if let Some(name) = names.into_iter().find(|name| prometheus_label(name) == *label) {
                    return Some(name);
                }
            }
            Err(err) => warn!(%err, "reading job names from the ops DB"),
        }
        Some(label.clone())
    }

    /// The tenant of a job, from the registry.
    pub async fn tenant(&self, job_name: &str) -> Option<String> {
        self.db.job(job_name).await.ok().flatten().map(|job| job.tenant)
    }
}

#[cfg(test)]
mod tests {
    use super::prometheus_label;

    #[test]
    fn rewrites_like_flink() {
        assert_eq!(prometheus_label("market-data-to-iceberg"), "market_data_to_iceberg");
        assert_eq!(prometheus_label("Source: market_data[30]"), "Source:_market_data_30_");
    }
}
