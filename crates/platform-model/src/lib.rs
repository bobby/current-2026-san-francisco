//! Event types for the `platform.*` Kafka topics (docs/design.md §5.1).
//!
//! Real telemetry (the collector, Vector, cicd-sim) and simulated fleet
//! telemetry (ShadowTraffic's `st-fleet`) share these schemas, so consumers
//! can't tell them apart. The JSON Schema files in `schemas/`, which the
//! ShadowTraffic configs target, are generated from these types: run
//! `UPDATE_SCHEMAS=1 cargo test -p platform-model` after changing them.
//!
//! Conventions:
//! - Every event is a JSON object keyed in Kafka by `job_name`, so each
//!   job's events stay in order. `platform.jobs` is compacted on that key.
//! - `job_name` is the platform's stable job name (e.g. `payments-enrichment`).
//!   `flink_job_id` is the Flink job ID of one run, which changes on every
//!   deploy. Prometheus rewrites `job_name` labels (`payments_enrichment`);
//!   events never do.
//! - Timestamps are RFC 3339 strings in UTC.
//! - `event_id` is a UUID. Producers that may emit the same event twice (the
//!   collector re-reads Flink's REST API after a restart) derive it from the
//!   event's natural key with [`event_id`], so the ops DB stores it once.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Timestamp = DateTime<Utc>;

/// Kafka topic names.
pub mod topics {
    pub const JOBS: &str = "platform.jobs";
    pub const DEPLOYMENTS: &str = "platform.deployments";
    pub const JOB_STATUS: &str = "platform.job-status";
    pub const CHECKPOINTS: &str = "platform.checkpoints";
    pub const LOGS: &str = "platform.logs";
    pub const METRICS: &str = "platform.metrics";
    pub const ALERTS: &str = "platform.alerts";
    pub const SIGNALS: &str = "platform.signals";
    pub const ACTIONS: &str = "platform.actions";
}

/// Namespace for deterministic event IDs (UUID v5).
const EVENT_ID_NAMESPACE: Uuid = Uuid::from_u128(0x6a0c_51d2_4c1e_4f7a_9b0e_2b3d_8e61_c0de);

/// A deterministic event ID derived from an event's natural key, e.g.
/// `event_id(&["checkpoint", flink_job_id, "42"])`.
pub fn event_id(key: &[&str]) -> Uuid {
    Uuid::new_v5(&EVENT_ID_NAMESPACE, key.join("\u{1f}").as_bytes())
}

/// `platform.jobs`: the job registry, compacted on `job_name`. Written by
/// cicd-sim on every deploy, and by `st-fleet` for the simulated fleet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobRecord {
    pub job_name: String,
    /// Owning tenant, e.g. `payments-eng`.
    pub tenant: String,
    /// Owning team's contact, e.g. a Mattermost handle or group.
    pub owner: String,
    pub tier: Tier,
    pub kind: JobKind,
    pub current_version: String,
    /// Flink job ID of the current run, if it is running.
    pub flink_job_id: Option<String>,
    /// Maximum parallelism the tenant's policy allows (rescale guardrail).
    pub max_parallelism: u32,
    pub deployed_at: Timestamp,
    pub first_deployed_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Tier {
    /// Business-critical: pages someone.
    Tier1,
    Tier2,
    /// Best effort.
    Tier3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    Java,
    Sql,
}

/// `platform.deployments`: one event per deploy or rollback (docs/design.md §4.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Deployment {
    pub event_id: Uuid,
    pub ts: Timestamp,
    pub job_name: String,
    pub tenant: String,
    pub kind: DeploymentKind,
    /// The version that was running before, if any.
    pub from_version: Option<String>,
    pub to_version: String,
    pub commit_sha: String,
    pub author: String,
    /// The change summary from the version catalogue.
    pub summary: String,
    /// Job arguments the version ran with (Java jobs), including any the
    /// request added, e.g. `--on-deser-error=dlq`.
    #[serde(default)]
    pub args: Vec<String>,
    /// Who asked for it: `director`, `operator`, `chatops-bot`, `user:<name>`.
    pub triggered_by: String,
    pub outcome: DeploymentOutcome,
    /// Why the deploy failed, if it did.
    pub failure_reason: Option<String>,
    /// Flink job ID of the new run, once submitted.
    pub flink_job_id: Option<String>,
    /// Savepoint or retained checkpoint the new run started from.
    pub restored_from: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentKind {
    Deploy,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentOutcome {
    Succeeded,
    Failed,
}

/// Flink job states, as the REST API reports them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobState {
    Initializing,
    Created,
    Running,
    Failing,
    Failed,
    Cancelling,
    Canceled,
    Finished,
    Restarting,
    Suspended,
    Reconciling,
}

impl JobState {
    /// The state has ended the run.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Failed | Self::Canceled | Self::Finished | Self::Suspended)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Initializing => "INITIALIZING",
            Self::Created => "CREATED",
            Self::Running => "RUNNING",
            Self::Failing => "FAILING",
            Self::Failed => "FAILED",
            Self::Cancelling => "CANCELLING",
            Self::Canceled => "CANCELED",
            Self::Finished => "FINISHED",
            Self::Restarting => "RESTARTING",
            Self::Suspended => "SUSPENDED",
            Self::Reconciling => "RECONCILING",
        }
    }
}

/// `platform.job-status`: a job's state transitions and failures. A failure
/// that triggers a restart is one event, with `failure` set and `status`
/// `RESTARTING` (or `FAILED` once restarts are exhausted).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct JobStatusEvent {
    pub event_id: Uuid,
    pub ts: Timestamp,
    pub job_name: String,
    pub flink_job_id: String,
    pub status: JobState,
    /// The state before this transition, if known.
    pub previous_status: Option<JobState>,
    /// Restarts of this run so far (Flink's `numRestarts`).
    pub restarts: u64,
    pub failure: Option<Failure>,
}

/// The exception behind a failure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Failure {
    /// Class of the top-level exception, e.g. `java.io.IOException`.
    pub exception_class: String,
    pub message: Option<String>,
    /// Class of the innermost `Caused by:` exception, which usually names the
    /// real problem (e.g. `java.lang.NullPointerException`).
    pub root_cause_class: Option<String>,
    pub root_cause_message: Option<String>,
    /// The first lines of the stack trace.
    pub stack_head: Option<String>,
    /// The task that failed, if the failure was in a task.
    pub task: Option<String>,
}

/// `platform.checkpoints`: completed and failed checkpoints and savepoints.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointEvent {
    pub event_id: Uuid,
    /// When the checkpoint completed or failed.
    pub ts: Timestamp,
    pub job_name: String,
    pub flink_job_id: String,
    pub checkpoint_id: u64,
    pub status: CheckpointStatus,
    pub is_savepoint: bool,
    pub trigger_ts: Timestamp,
    /// End-to-end duration in milliseconds.
    pub duration_ms: u64,
    /// Bytes written by this checkpoint (Flink's `checkpointed_size`).
    pub checkpointed_size_bytes: u64,
    /// Full state size in bytes (Flink's `state_size`).
    pub state_size_bytes: u64,
    pub failure_reason: Option<String>,
    /// Where a completed checkpoint or savepoint is stored, for restoring
    /// from it (rollbacks).
    pub external_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStatus {
    Completed,
    Failed,
}

/// `platform.logs`: structured log events from a job's JobManager and
/// TaskManager logs (warnings and errors).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct LogEvent {
    pub event_id: Uuid,
    pub ts: Timestamp,
    pub job_name: String,
    pub flink_job_id: Option<String>,
    pub level: LogLevel,
    /// `jobmanager`, `taskmanager` or `sql-gateway`.
    pub component: String,
    pub host: String,
    pub logger: String,
    pub message: String,
    pub exception_class: Option<String>,
    pub exception_message: Option<String>,
    pub root_cause_class: Option<String>,
    pub root_cause_message: Option<String>,
    pub stack_head: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
        }
    }
}

/// `platform.metrics`: one simulated metric sample. Real metrics are scraped
/// straight from Flink by Prometheus; the fleet-exporter serves these with
/// the same names and labels (docs/flink-metrics.md).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MetricSample {
    pub ts: Timestamp,
    /// A Flink metric name, e.g. `flink_jobmanager_job_numRestarts`.
    pub name: String,
    pub value: f64,
    /// Prometheus labels, with values rewritten as Flink's reporter does.
    pub labels: BTreeMap<String, String>,
}

/// `platform.alerts`: an alert firing or resolving, from Alertmanager.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AlertEvent {
    pub event_id: Uuid,
    /// When the alert started firing (`firing`) or resolved (`resolved`).
    pub ts: Timestamp,
    pub status: AlertStatus,
    pub alertname: String,
    /// Alertmanager's fingerprint: the same for every event of one alert.
    pub fingerprint: String,
    pub severity: Option<String>,
    /// The platform job name, if the alert is about one job.
    pub job_name: Option<String>,
    pub tenant: Option<String>,
    pub starts_at: Timestamp,
    pub ends_at: Option<Timestamp>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub generator_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AlertStatus {
    Firing,
    Resolved,
}

/// `platform.signals`: the triage agent's verdict on an alert or a group of
/// alerts (Scenario 1, docs/design.md §8.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Signal {
    pub signal_id: Uuid,
    pub ts: Timestamp,
    pub job_name: String,
    pub tenant: String,
    /// The alerts this verdict covers.
    pub alert_event_ids: Vec<Uuid>,
    pub disposition: Disposition,
    /// The incident this signal raised or was attached to.
    pub incident_id: Option<Uuid>,
    pub failure_class: Option<FailureClass>,
    /// Jev's probability that the problem is genuine and needs action.
    pub actionable: Option<f64>,
    /// Jev's user-impact score, from 0 (none) to 1 (stopped / data at risk).
    pub user_impact: Option<f64>,
    /// Jev's probability that the latest deployment caused this.
    pub deploy_related: Option<f64>,
    /// The deployment considered, if one was in the window.
    pub deployment_event_id: Option<Uuid>,
    /// The state, questions and answers sent to and from Jev, for audit.
    pub judgment: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Suppress,
    Attach,
    Raise,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    CodeRegression,
    BadInputData,
    ResourceSaturation,
    StateGrowth,
    SchemaMismatch,
    PlatformInfrastructure,
    TransientOrExpected,
    Unknown,
}

/// `platform.actions`: the audit log of every remediation proposed, approved,
/// rejected or executed, by an agent, a human or cicd-sim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ActionEvent {
    pub event_id: Uuid,
    pub ts: Timestamp,
    /// Groups the events of one action (proposed → approved → executed).
    pub action_id: Uuid,
    pub job_name: String,
    pub incident_id: Option<Uuid>,
    pub signal_id: Option<Uuid>,
    /// Who did it: `operator`, `chatops-bot`, `cicd-sim`, or `user:<name>`.
    pub actor: String,
    pub action: ActionKind,
    pub phase: ActionPhase,
    pub params: serde_json::Value,
    pub rationale: Option<String>,
    pub ticket: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    RollbackToPreviousVersion,
    RestartFromCheckpoint,
    ScaleUp,
    Redeploy,
    OpenTicket,
    UpdateTicket,
    EscalateToOwner,
    EscalateToPlatform,
    Observe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActionPhase {
    Proposed,
    Approved,
    Rejected,
    Executed,
    Failed,
    Verified,
}

/// The JSON Schema for each topic's events, as `(topic, schema)`.
pub fn schemas() -> Vec<(&'static str, schemars::Schema)> {
    use schemars::schema_for;
    vec![
        (topics::JOBS, schema_for!(JobRecord)),
        (topics::DEPLOYMENTS, schema_for!(Deployment)),
        (topics::JOB_STATUS, schema_for!(JobStatusEvent)),
        (topics::CHECKPOINTS, schema_for!(CheckpointEvent)),
        (topics::LOGS, schema_for!(LogEvent)),
        (topics::METRICS, schema_for!(MetricSample)),
        (topics::ALERTS, schema_for!(AlertEvent)),
        (topics::SIGNALS, schema_for!(Signal)),
        (topics::ACTIONS, schema_for!(ActionEvent)),
    ]
}
