//! A client for the parts of the Flink REST API the platform reads.

use std::collections::HashMap;

use platform_model::JobState;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Flink REST request failed: {0}")]
    Http(#[from] reqwest::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct FlinkClient {
    base_url: String,
    http: reqwest::Client,
}

/// One job in `GET /jobs/overview`.
#[derive(Debug, Clone, Deserialize)]
pub struct JobOverview {
    pub jid: String,
    pub name: String,
    pub state: JobState,
    #[serde(rename = "start-time")]
    pub start_time: i64,
    /// When the job last changed state, in epoch ms.
    #[serde(rename = "last-modification")]
    pub last_modification: i64,
}

/// One entry in `GET /jobs/:jid/exceptions`' exception history.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExceptionEntry {
    pub exception_name: String,
    pub stacktrace: String,
    /// Epoch ms.
    pub timestamp: i64,
    pub task_name: Option<String>,
}

/// One entry in `GET /jobs/:jid/checkpoints`' history.
#[derive(Debug, Clone, Deserialize)]
pub struct CheckpointEntry {
    pub id: u64,
    pub status: CheckpointState,
    pub is_savepoint: bool,
    pub trigger_timestamp: i64,
    pub latest_ack_timestamp: i64,
    pub failure_timestamp: Option<i64>,
    pub failure_message: Option<String>,
    pub end_to_end_duration: u64,
    pub checkpointed_size: u64,
    pub state_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckpointState {
    InProgress,
    Completed,
    Failed,
}

impl FlinkClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(10)).build().expect("HTTP client");
        Self { base_url: base_url.into().trim_end_matches('/').to_string(), http }
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = format!("{}{path}", self.base_url);
        Ok(self.http.get(url).send().await?.error_for_status()?.json().await?)
    }

    /// Every job the cluster knows, including recently terminated ones.
    pub async fn jobs(&self) -> Result<Vec<JobOverview>> {
        #[derive(Deserialize)]
        struct Overview {
            jobs: Vec<JobOverview>,
        }
        Ok(self.get::<Overview>("/jobs/overview").await?.jobs)
    }

    /// The job's recent failures, newest first.
    pub async fn exceptions(&self, jid: &str, max: usize) -> Result<Vec<ExceptionEntry>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Exceptions {
            exception_history: History,
        }
        #[derive(Deserialize)]
        struct History {
            entries: Vec<ExceptionEntry>,
        }
        let path = format!("/jobs/{jid}/exceptions?maxExceptions={max}");
        Ok(self.get::<Exceptions>(&path).await?.exception_history.entries)
    }

    /// The job's recent checkpoints and savepoints, newest first.
    pub async fn checkpoints(&self, jid: &str) -> Result<Vec<CheckpointEntry>> {
        #[derive(Deserialize)]
        struct Checkpoints {
            #[serde(default)]
            history: Vec<CheckpointEntry>,
        }
        Ok(self.get::<Checkpoints>(&format!("/jobs/{jid}/checkpoints")).await?.history)
    }

    /// Job-scoped JobManager metrics, e.g. `numRestarts`. The JobManager
    /// fetches metrics lazily, so a metric can be missing on the first call.
    pub async fn job_metrics(&self, jid: &str, names: &[&str]) -> Result<HashMap<String, f64>> {
        #[derive(Deserialize)]
        struct Metric {
            id: String,
            value: String,
        }
        let path = format!("/jobs/{jid}/metrics?get={}", names.join(","));
        let metrics: Vec<Metric> = self.get(&path).await?;
        Ok(metrics.into_iter().filter_map(|m| Some((m.id, m.value.parse().ok()?))).collect())
    }
}
