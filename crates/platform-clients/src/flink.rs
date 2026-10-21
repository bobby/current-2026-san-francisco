//! A client for the parts of the Flink REST API the platform reads.

use std::collections::HashMap;

use platform_model::JobState;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Flink REST request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("reading {path}: {source}")]
    Io { path: String, source: std::io::Error },
    #[error("Flink rejected the request: {0}")]
    Rejected(String),
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
    pub external_path: Option<String>,
}

/// The outcome of an asynchronous savepoint (stop-with-savepoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SavepointStatus {
    InProgress,
    Completed { location: String },
    Failed { cause: String },
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

    async fn send<T: for<'de> Deserialize<'de>>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Rejected(format!("{status}: {body}")));
        }
        Ok(response.json().await?)
    }

    /// Every job the cluster knows, including recently terminated ones.
    pub async fn jobs(&self) -> Result<Vec<JobOverview>> {
        #[derive(Deserialize)]
        struct Overview {
            jobs: Vec<JobOverview>,
        }
        Ok(self.get::<Overview>("/jobs/overview").await?.jobs)
    }

    /// The newest run of a job by name that hasn't terminated.
    pub async fn active_job(&self, name: &str) -> Result<Option<JobOverview>> {
        let mut jobs: Vec<JobOverview> =
            self.jobs().await?.into_iter().filter(|j| j.name == name && !j.state.is_terminal()).collect();
        jobs.sort_by_key(|j| std::cmp::Reverse(j.start_time));
        Ok(jobs.into_iter().next())
    }

    /// A job's current state, if Flink knows it.
    pub async fn job_state(&self, jid: &str) -> Result<Option<JobState>> {
        Ok(self.jobs().await?.into_iter().find(|j| j.jid == jid).map(|j| j.state))
    }

    /// Uploads a jar and returns its ID for [`Self::run_jar`].
    pub async fn upload_jar(&self, path: &std::path::Path) -> Result<String> {
        #[derive(Deserialize)]
        struct Uploaded {
            filename: String,
        }
        let bytes =
            tokio::fs::read(path).await.map_err(|source| Error::Io { path: path.display().to_string(), source })?;
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(path.file_name().unwrap_or_default().to_string_lossy().into_owned())
            .mime_str("application/x-java-archive")?;
        let form = reqwest::multipart::Form::new().part("jarfile", part);
        let uploaded: Uploaded =
            self.send(self.http.post(format!("{}/jars/upload", self.base_url)).multipart(form)).await?;
        Ok(uploaded.filename.rsplit('/').next().unwrap_or_default().to_string())
    }

    /// Runs an uploaded jar, optionally restoring from a savepoint or
    /// retained checkpoint, and returns the new job's ID.
    pub async fn run_jar(
        &self,
        jar_id: &str,
        entry_class: &str,
        args: &[String],
        restore_from: Option<&str>,
    ) -> Result<String> {
        #[derive(Deserialize)]
        struct Run {
            jobid: String,
        }
        let body = serde_json::json!({
            "entryClass": entry_class,
            "programArgsList": args,
            "savepointPath": restore_from,
            "allowNonRestoredState": true,
        });
        let url = format!("{}/jars/{jar_id}/run", self.base_url);
        Ok(self.send::<Run>(self.http.post(url).json(&body)).await?.jobid)
    }

    pub async fn delete_jar(&self, jar_id: &str) -> Result<()> {
        self.http.delete(format!("{}/jars/{jar_id}", self.base_url)).send().await?.error_for_status()?;
        Ok(())
    }

    /// Starts a stop-with-savepoint (to the cluster's default savepoint
    /// directory) and returns the trigger ID for [`Self::savepoint_status`].
    pub async fn stop_with_savepoint(&self, jid: &str) -> Result<String> {
        #[derive(Deserialize)]
        struct Trigger {
            #[serde(rename = "request-id")]
            request_id: String,
        }
        let url = format!("{}/jobs/{jid}/stop", self.base_url);
        let trigger: Trigger = self.send(self.http.post(url).json(&serde_json::json!({"drain": false}))).await?;
        Ok(trigger.request_id)
    }

    pub async fn savepoint_status(&self, jid: &str, trigger_id: &str) -> Result<SavepointStatus> {
        let status: serde_json::Value = self.get(&format!("/jobs/{jid}/savepoints/{trigger_id}")).await?;
        if status["status"]["id"] != "COMPLETED" {
            return Ok(SavepointStatus::InProgress);
        }
        let operation = &status["operation"];
        Ok(match operation["location"].as_str() {
            Some(location) => SavepointStatus::Completed { location: location.to_string() },
            None => {
                let trace = operation["failure-cause"]["stack-trace"].as_str().unwrap_or("unknown failure");
                SavepointStatus::Failed { cause: trace.lines().next().unwrap_or_default().to_string() }
            }
        })
    }

    pub async fn cancel(&self, jid: &str) -> Result<()> {
        self.http.patch(format!("{}/jobs/{jid}?mode=cancel", self.base_url)).send().await?.error_for_status()?;
        Ok(())
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
