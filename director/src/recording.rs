//! A recorded run (docs/design.md §10.3): what the audience saw, saved so
//! `director replay` can show it again without the agents, Jev or Claude.
//!
//! - the chatops-bot's transcript of every post and edit in the platform
//!   channels, in order;
//! - the rows Grafana and the tickets UI read: the ops DB's `alerts`,
//!   `signals`, `deployments` and `actions` (the dashboard's counters and
//!   annotations), and the tickets' `incident` and `journal`;
//! - the real jobs' Flink metrics that Grafana's per-job panels chart, from
//!   Prometheus.
//!
//! Times are kept as recorded; the run's start is offset zero. A recording
//! is one gzipped JSON file, `recordings/<name>.json.gz`.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::Path,
    time::Duration,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use platform_clients::{chatops::TranscriptEntry, prometheus::PrometheusClient};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Connection, PgConnection};

use crate::state::StepRecord;

pub const FORMAT: u32 = 1;

/// The Flink metrics behind Grafana's per-job panels
/// (`observability/grafana/dashboards/platform-overview.json`).
pub const METRICS: [&str; 5] = [
    "flink_jobmanager_job_numRestarts",
    "flink_jobmanager_job_lastCheckpointDuration",
    "flink_jobmanager_job_lastCheckpointSize",
    "flink_taskmanager_job_task_operator_pendingRecords",
    "flink_taskmanager_job_task_busyTimeMsPerSecond",
];

/// Prometheus scrapes every 5 s; the recording keeps a point per step.
pub const STEP: Duration = Duration::from_secs(5);

/// The tables a replay fills: (database, table, the column that says when a
/// row appeared). Parents before children: a journal entry needs its ticket.
pub const TABLES: [(Database, &str, &str); 6] = [
    (Database::Ops, "alerts", "ts"),
    (Database::Ops, "signals", "ts"),
    (Database::Ops, "deployments", "ts"),
    (Database::Ops, "actions", "ts"),
    (Database::Tickets, "incident", "sys_created_on"),
    (Database::Tickets, "journal", "created_on"),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Database {
    Ops,
    Tickets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recording {
    pub format: u32,
    pub recorded_at: DateTime<Utc>,
    /// Where it was recorded, e.g. "1000-job fleet".
    pub fleet: String,
    pub seed: u64,
    pub scenario: String,
    /// The run's start: offset zero.
    pub started_at: DateTime<Utc>,
    /// The last moment recorded.
    pub ends_at: DateTime<Utc>,
    /// The timeline's steps, as the run went.
    pub steps: Vec<StepRecord>,
    pub transcript: Vec<TranscriptEntry>,
    pub tables: Vec<Table>,
    pub series: Vec<Series>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub database: Database,
    pub table: String,
    pub time_column: String,
    /// Every `timestamptz` column, shifted together on replay.
    pub timestamp_columns: Vec<String>,
    /// Each row as `row_to_json` renders it, in time order.
    pub rows: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    /// The series' labels, with `__name__`, without Prometheus's `job` and
    /// `instance`.
    pub labels: BTreeMap<String, String>,
    /// (seconds from the run's start, value).
    pub points: Vec<(f64, f64)>,
}

impl Recording {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let mut json = String::new();
        flate2::read::GzDecoder::new(file)
            .read_to_string(&mut json)
            .with_context(|| format!("reading {}", path.display()))?;
        let recording: Self = serde_json::from_str(&json).with_context(|| format!("parsing {}", path.display()))?;
        anyhow::ensure!(recording.format == FORMAT, "{} has format {}, not {FORMAT}", path.display(), recording.format);
        Ok(recording)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("tmp");
        let mut gz = flate2::write::GzEncoder::new(std::fs::File::create(&tmp)?, flate2::Compression::best());
        gz.write_all(without_account_ids(&serde_json::to_string(self)?).as_bytes())?;
        gz.finish()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Seconds from the run's start.
    pub fn offset(&self, ts: DateTime<Utc>) -> f64 {
        (ts - self.started_at).num_milliseconds() as f64 / 1000.0
    }

    pub fn length(&self) -> f64 {
        self.offset(self.ends_at)
    }

    /// One line about it, e.g. for `replay status`.
    pub fn describe(&self) -> String {
        let posts = self.transcript.iter().filter(|e| !e.edit).count();
        let rows: usize = self.tables.iter().map(|t| t.rows.len()).sum();
        format!(
            "run of scenario {} on the {} (seed {}), started {}: {} long, {posts} posts and {} edits, {rows} rows, {} series",
            self.scenario,
            self.fleet,
            self.seed,
            self.started_at.format("%Y-%m-%d %H:%M UTC"),
            crate::clock(Duration::from_secs_f64(self.length().max(0.0))),
            self.transcript.len() - posts,
            self.series.len(),
        )
    }
}

/// Recordings are committed to a public repo, and on AWS the bucket that
/// checkpoints and savepoints live in is named `<stack>-<account>-<region>`
/// (`infra/cloudformation/demo.yaml`). This replaces the 12-digit account ID
/// in every `s3://` bucket name with `account`.
fn without_account_ids(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    let mut rest = json;
    while let Some(at) = rest.find("s3://") {
        let start = at + "s3://".len();
        out.push_str(&rest[..start]);
        let len = rest[start..]
            .find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-'))
            .unwrap_or(rest.len() - start);
        let bucket = &rest[start..start + len];
        for (i, part) in bucket.split('-').enumerate() {
            if i > 0 {
                out.push('-');
            }
            let account = part.len() == 12 && part.bytes().all(|b| b.is_ascii_digit());
            out.push_str(if account { "account" } else { part });
        }
        rest = &rest[start + len..];
    }
    out.push_str(rest);
    out
}

/// Reads one table's rows with `time_column` in `[from, to]`.
pub async fn read_table(
    url: &str,
    database: Database,
    table: &str,
    time_column: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<Table> {
    let mut db = PgConnection::connect(url).await.with_context(|| format!("connecting to the {database:?} DB"))?;
    let timestamp_columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name::text FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = $1 AND data_type = 'timestamp with time zone'
         ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(&mut db)
    .await?;
    anyhow::ensure!(
        timestamp_columns.iter().any(|c| c == time_column),
        "{table} has no timestamp column {time_column}"
    );
    // `table` and `time_column` come from TABLES, not from input.
    let sql =
        format!("SELECT row_to_json(t) FROM {table} t WHERE {time_column} BETWEEN $1 AND $2 ORDER BY {time_column}");
    let rows: Vec<Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql)).bind(from).bind(to).fetch_all(&mut db).await?;
    Ok(Table { database, table: table.to_string(), time_column: time_column.to_string(), timestamp_columns, rows })
}

/// The real jobs' series behind Grafana's per-job panels over `[from, to]`.
/// `jobs` are cicd-sim's catalogue names; Flink's labels use `_` for `-`.
pub async fn read_series(
    prometheus_url: &str,
    jobs: &[String],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> anyhow::Result<Vec<Series>> {
    let prometheus = PrometheusClient::new(prometheus_url);
    let labels: Vec<String> = jobs.iter().map(|j| platform_clients::prometheus::job_label(j)).collect();
    let selector = format!(r#"{{__name__=~"{}", job_name=~"{}"}}"#, METRICS.join("|"), labels.join("|"));
    // A range query returns at most 11,000 points per series; a few hours'
    // worth at this step.
    let mut series = Vec::new();
    let mut start = from;
    while start < to {
        let end = (start + chrono::Duration::hours(4)).min(to);
        for s in prometheus.query_range(&selector, start, end, STEP).await.context("querying Prometheus")? {
            let mut labels = s.labels;
            labels.remove("job");
            labels.remove("instance");
            let points = s.points.iter().map(|(t, v)| ((*t - from).num_milliseconds() as f64 / 1000.0, *v));
            match series.iter_mut().find(|x: &&mut Series| x.labels == labels) {
                Some(existing) => existing.points.extend(points),
                None => series.push(Series { labels, points: points.collect() }),
            }
        }
        start = end;
    }
    Ok(series)
}

#[cfg(test)]
mod tests {
    use super::without_account_ids;

    #[test]
    fn account_ids_leave_bucket_names() {
        assert_eq!(
            without_account_ids(
                r#"{"restored_from":"s3://current2026-demo-123456789012-us-east-1/flink/savepoints/savepoint-2ac748-a403b02de890"}"#
            ),
            r#"{"restored_from":"s3://current2026-demo-account-us-east-1/flink/savepoints/savepoint-2ac748-a403b02de890"}"#,
        );
        // Local buckets, other numbers and paths outside the bucket are kept.
        let kept = r#"["s3://flink/checkpoints/123456789012", "job 123456789012", "s3://flink-01", "s3://"]"#;
        assert_eq!(without_account_ids(kept), kept);
    }
}
