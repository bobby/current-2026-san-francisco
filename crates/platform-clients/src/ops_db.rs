//! The ops DB (docs/design.md §5.4): the queryable history of the `platform.*`
//! topics. The collector writes it; agents and the MCP server read it,
//! mostly with point-in-time queries ("what did checkpoints look like before
//! the first alert?").
//!
//! Reads return the same `platform_model` types as the topics.

use std::collections::BTreeMap;

use platform_model::{
    ActionEvent, AlertEvent, CheckpointEvent, Deployment, Failure, JobRecord, JobStatusEvent, LogEvent, LogLevel,
    Signal, Timestamp,
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions, types::Json};
use uuid::Uuid;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("ops DB query failed: {0}")]
    Db(#[from] sqlx::Error),
    #[error("ops DB migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("ops DB holds a value the model doesn't know: {0}")]
    Decode(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct OpsDb {
    pool: PgPool,
}

/// A job's state at a point in time, assembled from its history.
#[derive(Debug, Clone, PartialEq)]
pub struct JobSnapshot {
    pub job_name: String,
    pub at: Timestamp,
    /// The latest status event at or before `at`.
    pub status: Option<JobStatusEvent>,
    /// The latest deployment at or before `at`.
    pub deployment: Option<Deployment>,
    /// The latest completed checkpoint at or before `at`.
    pub last_completed_checkpoint: Option<CheckpointEvent>,
    /// Failed checkpoints since the last completed one.
    pub failed_checkpoints_since: i64,
}

/// Stores an enum as the string serde gives it, e.g. `RESTARTING`.
fn enum_str<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(s)) => s,
        other => panic!("not a unit enum: {other:?}"),
    }
}

fn parse_enum<T: DeserializeOwned>(s: &str) -> Result<T> {
    Ok(serde_json::from_value(serde_json::Value::String(s.to_string()))?)
}

impl OpsDb {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        Ok(MIGRATOR.run(&self.pool).await?)
    }

    // Writes. Each is idempotent: re-inserting an event changes nothing.

    pub async fn upsert_job(&self, job: &JobRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO jobs (job_name, tenant, owner, tier, kind, current_version, flink_job_id,
                               max_parallelism, deployed_at, first_deployed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (job_name) DO UPDATE SET
               tenant = EXCLUDED.tenant, owner = EXCLUDED.owner, tier = EXCLUDED.tier, kind = EXCLUDED.kind,
               current_version = EXCLUDED.current_version, flink_job_id = EXCLUDED.flink_job_id,
               max_parallelism = EXCLUDED.max_parallelism, deployed_at = EXCLUDED.deployed_at,
               first_deployed_at = EXCLUDED.first_deployed_at, updated_at = now()",
        )
        .bind(&job.job_name)
        .bind(&job.tenant)
        .bind(&job.owner)
        .bind(enum_str(&job.tier))
        .bind(enum_str(&job.kind))
        .bind(&job.current_version)
        .bind(&job.flink_job_id)
        .bind(job.max_parallelism as i32)
        .bind(job.deployed_at)
        .bind(job.first_deployed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Handles a tombstone on the compacted `platform.jobs` topic.
    pub async fn delete_job(&self, job_name: &str) -> Result<()> {
        sqlx::query("DELETE FROM jobs WHERE job_name = $1").bind(job_name).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn insert_deployment(&self, d: &Deployment) -> Result<()> {
        sqlx::query(
            "INSERT INTO deployments (event_id, ts, job_name, tenant, kind, from_version, to_version,
                                      commit_sha, author, summary, outcome, flink_job_id, restored_from)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT DO NOTHING",
        )
        .bind(d.event_id)
        .bind(d.ts)
        .bind(&d.job_name)
        .bind(&d.tenant)
        .bind(enum_str(&d.kind))
        .bind(&d.from_version)
        .bind(&d.to_version)
        .bind(&d.commit_sha)
        .bind(&d.author)
        .bind(&d.summary)
        .bind(enum_str(&d.outcome))
        .bind(&d.flink_job_id)
        .bind(&d.restored_from)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_job_status(&self, e: &JobStatusEvent) -> Result<()> {
        let f = e.failure.as_ref();
        sqlx::query(
            "INSERT INTO job_status (event_id, ts, job_name, flink_job_id, status, previous_status, restarts,
                                     exception_class, exception_message, root_cause_class, root_cause_message,
                                     stack_head, task)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT DO NOTHING",
        )
        .bind(e.event_id)
        .bind(e.ts)
        .bind(&e.job_name)
        .bind(&e.flink_job_id)
        .bind(e.status.as_str())
        .bind(e.previous_status.map(|s| s.as_str()))
        .bind(e.restarts as i64)
        .bind(f.map(|f| &f.exception_class))
        .bind(f.and_then(|f| f.message.as_ref()))
        .bind(f.and_then(|f| f.root_cause_class.as_ref()))
        .bind(f.and_then(|f| f.root_cause_message.as_ref()))
        .bind(f.and_then(|f| f.stack_head.as_ref()))
        .bind(f.and_then(|f| f.task.as_ref()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_checkpoint(&self, c: &CheckpointEvent) -> Result<()> {
        sqlx::query(
            "INSERT INTO checkpoints (event_id, ts, job_name, flink_job_id, checkpoint_id, status, is_savepoint,
                                      trigger_ts, duration_ms, checkpointed_size_bytes, state_size_bytes,
                                      failure_reason)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT DO NOTHING",
        )
        .bind(c.event_id)
        .bind(c.ts)
        .bind(&c.job_name)
        .bind(&c.flink_job_id)
        .bind(c.checkpoint_id as i64)
        .bind(enum_str(&c.status))
        .bind(c.is_savepoint)
        .bind(c.trigger_ts)
        .bind(c.duration_ms as i64)
        .bind(c.checkpointed_size_bytes as i64)
        .bind(c.state_size_bytes as i64)
        .bind(&c.failure_reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_log(&self, l: &LogEvent) -> Result<()> {
        sqlx::query(
            "INSERT INTO logs (event_id, ts, job_name, flink_job_id, level, component, host, logger, message,
                               exception_class, exception_message, root_cause_class, root_cause_message,
                               stack_head)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
             ON CONFLICT DO NOTHING",
        )
        .bind(l.event_id)
        .bind(l.ts)
        .bind(&l.job_name)
        .bind(&l.flink_job_id)
        .bind(l.level.as_str())
        .bind(&l.component)
        .bind(&l.host)
        .bind(&l.logger)
        .bind(&l.message)
        .bind(&l.exception_class)
        .bind(&l.exception_message)
        .bind(&l.root_cause_class)
        .bind(&l.root_cause_message)
        .bind(&l.stack_head)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_alert(&self, a: &AlertEvent) -> Result<()> {
        sqlx::query(
            "INSERT INTO alerts (event_id, ts, status, alertname, fingerprint, severity, job_name, tenant,
                                 starts_at, ends_at, labels, annotations, generator_url)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
             ON CONFLICT DO NOTHING",
        )
        .bind(a.event_id)
        .bind(a.ts)
        .bind(enum_str(&a.status))
        .bind(&a.alertname)
        .bind(&a.fingerprint)
        .bind(&a.severity)
        .bind(&a.job_name)
        .bind(&a.tenant)
        .bind(a.starts_at)
        .bind(a.ends_at)
        .bind(Json(&a.labels))
        .bind(Json(&a.annotations))
        .bind(&a.generator_url)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_signal(&self, s: &Signal) -> Result<()> {
        sqlx::query(
            "INSERT INTO signals (signal_id, ts, job_name, tenant, disposition, incident_id, failure_class, event)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT DO NOTHING",
        )
        .bind(s.signal_id)
        .bind(s.ts)
        .bind(&s.job_name)
        .bind(&s.tenant)
        .bind(enum_str(&s.disposition))
        .bind(s.incident_id)
        .bind(s.failure_class.as_ref().map(enum_str))
        .bind(Json(s))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_action(&self, a: &ActionEvent) -> Result<()> {
        sqlx::query(
            "INSERT INTO actions (event_id, ts, action_id, job_name, incident_id, actor, action, phase, event)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT DO NOTHING",
        )
        .bind(a.event_id)
        .bind(a.ts)
        .bind(a.action_id)
        .bind(&a.job_name)
        .bind(a.incident_id)
        .bind(&a.actor)
        .bind(enum_str(&a.action))
        .bind(enum_str(&a.phase))
        .bind(Json(a))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Deletes logs older than `cutoff` (the retention window). Returns the
    /// number of rows deleted.
    pub async fn delete_logs_before(&self, cutoff: Timestamp) -> Result<u64> {
        let result = sqlx::query("DELETE FROM logs WHERE ts < $1").bind(cutoff).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    // Reads. Point-in-time queries take an upper bound and return the
    // newest rows first.

    pub async fn job(&self, job_name: &str) -> Result<Option<JobRecord>> {
        let row: Option<JobRow> =
            sqlx::query_as("SELECT * FROM jobs WHERE job_name = $1").bind(job_name).fetch_optional(&self.pool).await?;
        row.map(TryInto::try_into).transpose()
    }

    /// Every job name in the registry.
    pub async fn job_names(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar("SELECT job_name FROM jobs ORDER BY job_name").fetch_all(&self.pool).await?)
    }

    /// The job's checkpoints that finished before `before`, newest first.
    pub async fn checkpoints_before(
        &self,
        job_name: &str,
        before: Timestamp,
        limit: i64,
    ) -> Result<Vec<CheckpointEvent>> {
        let rows: Vec<CheckpointRow> = sqlx::query_as(
            "SELECT * FROM checkpoints WHERE job_name = $1 AND ts < $2 ORDER BY ts DESC, checkpoint_id DESC LIMIT $3",
        )
        .bind(job_name)
        .bind(before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// The job's deployments before `before`, newest first.
    pub async fn deployments_before(&self, job_name: &str, before: Timestamp, limit: i64) -> Result<Vec<Deployment>> {
        let rows: Vec<DeploymentRow> =
            sqlx::query_as("SELECT * FROM deployments WHERE job_name = $1 AND ts < $2 ORDER BY ts DESC LIMIT $3")
                .bind(job_name)
                .bind(before)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// The job's status events in `[from, to)`, oldest first.
    pub async fn status_between(&self, job_name: &str, from: Timestamp, to: Timestamp) -> Result<Vec<JobStatusEvent>> {
        let rows: Vec<JobStatusRow> =
            sqlx::query_as("SELECT * FROM job_status WHERE job_name = $1 AND ts >= $2 AND ts < $3 ORDER BY ts")
                .bind(job_name)
                .bind(from)
                .bind(to)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// The job's logs at `min_level` or above in `[from, to)`, newest first.
    pub async fn logs_between(
        &self,
        job_name: &str,
        from: Timestamp,
        to: Timestamp,
        min_level: LogLevel,
        limit: i64,
    ) -> Result<Vec<LogEvent>> {
        let levels: Vec<&str> =
            [LogLevel::Trace, LogLevel::Debug, LogLevel::Info, LogLevel::Warn, LogLevel::Error, LogLevel::Fatal]
                .into_iter()
                .filter(|l| *l >= min_level)
                .map(LogLevel::as_str)
                .collect();
        let rows: Vec<LogRow> = sqlx::query_as(
            "SELECT * FROM logs WHERE job_name = $1 AND ts >= $2 AND ts < $3 AND level = ANY($4)
             ORDER BY ts DESC LIMIT $5",
        )
        .bind(job_name)
        .bind(from)
        .bind(to)
        .bind(&levels)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// Alert events in `[from, to)`, for one job or all, oldest first.
    pub async fn alerts_between(
        &self,
        job_name: Option<&str>,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<AlertEvent>> {
        let rows: Vec<AlertRow> = sqlx::query_as(
            "SELECT * FROM alerts WHERE ($1::text IS NULL OR job_name = $1) AND ts >= $2 AND ts < $3 ORDER BY ts",
        )
        .bind(job_name)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }

    /// The job's state as of `at`: latest status, deployment and completed
    /// checkpoint, and how many checkpoints have failed since.
    pub async fn job_at(&self, job_name: &str, at: Timestamp) -> Result<JobSnapshot> {
        let status: Option<JobStatusRow> =
            sqlx::query_as("SELECT * FROM job_status WHERE job_name = $1 AND ts <= $2 ORDER BY ts DESC LIMIT 1")
                .bind(job_name)
                .bind(at)
                .fetch_optional(&self.pool)
                .await?;
        let deployment: Option<DeploymentRow> =
            sqlx::query_as("SELECT * FROM deployments WHERE job_name = $1 AND ts <= $2 ORDER BY ts DESC LIMIT 1")
                .bind(job_name)
                .bind(at)
                .fetch_optional(&self.pool)
                .await?;
        let checkpoint: Option<CheckpointRow> = sqlx::query_as(
            "SELECT * FROM checkpoints WHERE job_name = $1 AND ts <= $2 AND status = 'completed'
             ORDER BY ts DESC LIMIT 1",
        )
        .bind(job_name)
        .bind(at)
        .fetch_optional(&self.pool)
        .await?;
        let (failed_checkpoints_since,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM checkpoints WHERE job_name = $1 AND ts <= $2 AND status = 'failed'
               AND ($3::timestamptz IS NULL OR ts > $3)",
        )
        .bind(job_name)
        .bind(at)
        .bind(checkpoint.as_ref().map(|c| c.ts))
        .fetch_one(&self.pool)
        .await?;
        Ok(JobSnapshot {
            job_name: job_name.to_string(),
            at,
            status: status.map(TryInto::try_into).transpose()?,
            deployment: deployment.map(TryInto::try_into).transpose()?,
            last_completed_checkpoint: checkpoint.map(TryInto::try_into).transpose()?,
            failed_checkpoints_since,
        })
    }
}

// Row types, converted to the model types.

#[derive(FromRow)]
struct JobRow {
    job_name: String,
    tenant: String,
    owner: String,
    tier: String,
    kind: String,
    current_version: String,
    flink_job_id: Option<String>,
    max_parallelism: i32,
    deployed_at: Timestamp,
    first_deployed_at: Timestamp,
}

impl TryFrom<JobRow> for JobRecord {
    type Error = Error;
    fn try_from(r: JobRow) -> Result<Self> {
        Ok(Self {
            job_name: r.job_name,
            tenant: r.tenant,
            owner: r.owner,
            tier: parse_enum(&r.tier)?,
            kind: parse_enum(&r.kind)?,
            current_version: r.current_version,
            flink_job_id: r.flink_job_id,
            max_parallelism: r.max_parallelism as u32,
            deployed_at: r.deployed_at,
            first_deployed_at: r.first_deployed_at,
        })
    }
}

#[derive(FromRow)]
struct DeploymentRow {
    event_id: Uuid,
    ts: Timestamp,
    job_name: String,
    tenant: String,
    kind: String,
    from_version: Option<String>,
    to_version: String,
    commit_sha: String,
    author: String,
    summary: String,
    outcome: String,
    flink_job_id: Option<String>,
    restored_from: Option<String>,
}

impl TryFrom<DeploymentRow> for Deployment {
    type Error = Error;
    fn try_from(r: DeploymentRow) -> Result<Self> {
        Ok(Self {
            event_id: r.event_id,
            ts: r.ts,
            job_name: r.job_name,
            tenant: r.tenant,
            kind: parse_enum(&r.kind)?,
            from_version: r.from_version,
            to_version: r.to_version,
            commit_sha: r.commit_sha,
            author: r.author,
            summary: r.summary,
            outcome: parse_enum(&r.outcome)?,
            flink_job_id: r.flink_job_id,
            restored_from: r.restored_from,
        })
    }
}

#[derive(FromRow)]
struct JobStatusRow {
    event_id: Uuid,
    ts: Timestamp,
    job_name: String,
    flink_job_id: String,
    status: String,
    previous_status: Option<String>,
    restarts: i64,
    exception_class: Option<String>,
    exception_message: Option<String>,
    root_cause_class: Option<String>,
    root_cause_message: Option<String>,
    stack_head: Option<String>,
    task: Option<String>,
}

impl TryFrom<JobStatusRow> for JobStatusEvent {
    type Error = Error;
    fn try_from(r: JobStatusRow) -> Result<Self> {
        let failure = r.exception_class.map(|exception_class| Failure {
            exception_class,
            message: r.exception_message,
            root_cause_class: r.root_cause_class,
            root_cause_message: r.root_cause_message,
            stack_head: r.stack_head,
            task: r.task,
        });
        Ok(Self {
            event_id: r.event_id,
            ts: r.ts,
            job_name: r.job_name,
            flink_job_id: r.flink_job_id,
            status: parse_enum(&r.status)?,
            previous_status: r.previous_status.as_deref().map(parse_enum).transpose()?,
            restarts: r.restarts as u64,
            failure,
        })
    }
}

#[derive(FromRow)]
struct CheckpointRow {
    event_id: Uuid,
    ts: Timestamp,
    job_name: String,
    flink_job_id: String,
    checkpoint_id: i64,
    status: String,
    is_savepoint: bool,
    trigger_ts: Timestamp,
    duration_ms: i64,
    checkpointed_size_bytes: i64,
    state_size_bytes: i64,
    failure_reason: Option<String>,
}

impl TryFrom<CheckpointRow> for CheckpointEvent {
    type Error = Error;
    fn try_from(r: CheckpointRow) -> Result<Self> {
        Ok(Self {
            event_id: r.event_id,
            ts: r.ts,
            job_name: r.job_name,
            flink_job_id: r.flink_job_id,
            checkpoint_id: r.checkpoint_id as u64,
            status: parse_enum(&r.status)?,
            is_savepoint: r.is_savepoint,
            trigger_ts: r.trigger_ts,
            duration_ms: r.duration_ms as u64,
            checkpointed_size_bytes: r.checkpointed_size_bytes as u64,
            state_size_bytes: r.state_size_bytes as u64,
            failure_reason: r.failure_reason,
        })
    }
}

#[derive(FromRow)]
struct LogRow {
    event_id: Uuid,
    ts: Timestamp,
    job_name: String,
    flink_job_id: Option<String>,
    level: String,
    component: String,
    host: String,
    logger: String,
    message: String,
    exception_class: Option<String>,
    exception_message: Option<String>,
    root_cause_class: Option<String>,
    root_cause_message: Option<String>,
    stack_head: Option<String>,
}

impl TryFrom<LogRow> for LogEvent {
    type Error = Error;
    fn try_from(r: LogRow) -> Result<Self> {
        Ok(Self {
            event_id: r.event_id,
            ts: r.ts,
            job_name: r.job_name,
            flink_job_id: r.flink_job_id,
            level: parse_enum(&r.level)?,
            component: r.component,
            host: r.host,
            logger: r.logger,
            message: r.message,
            exception_class: r.exception_class,
            exception_message: r.exception_message,
            root_cause_class: r.root_cause_class,
            root_cause_message: r.root_cause_message,
            stack_head: r.stack_head,
        })
    }
}

#[derive(FromRow)]
struct AlertRow {
    event_id: Uuid,
    ts: Timestamp,
    status: String,
    alertname: String,
    fingerprint: String,
    severity: Option<String>,
    job_name: Option<String>,
    tenant: Option<String>,
    starts_at: Timestamp,
    ends_at: Option<Timestamp>,
    labels: Json<BTreeMap<String, String>>,
    annotations: Json<BTreeMap<String, String>>,
    generator_url: Option<String>,
}

impl TryFrom<AlertRow> for AlertEvent {
    type Error = Error;
    fn try_from(r: AlertRow) -> Result<Self> {
        Ok(Self {
            event_id: r.event_id,
            ts: r.ts,
            status: parse_enum(&r.status)?,
            alertname: r.alertname,
            fingerprint: r.fingerprint,
            severity: r.severity,
            job_name: r.job_name,
            tenant: r.tenant,
            starts_at: r.starts_at,
            ends_at: r.ends_at,
            labels: r.labels.0,
            annotations: r.annotations.0,
            generator_url: r.generator_url,
        })
    }
}
