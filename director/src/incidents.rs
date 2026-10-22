//! The incidents a run produced, from the ops DB: for each real job, its
//! first symptom since the run started, a failure or a firing alert; for each
//! of the fleet's genuine problems (crash loop, stalled, state growth), its
//! first failure. In order. Two runs with the same seed should list the same
//! incidents in the same order; the times will differ by seconds.
//!
//! The fleet's alerts aren't incidents. The naive rules fire on simulated
//! metrics that hover around their thresholds, so whether one crosses in a
//! given half hour depends on scrape timing, not on the seed. That noise is
//! what Scenario 1 triages; it isn't part of the timeline.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Connection, PgConnection};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Incident {
    pub ts: DateTime<Utc>,
    pub job_name: String,
    /// `failure` or `alert`.
    pub kind: String,
    /// The root-cause exception class, or the alert's name.
    pub detail: String,
}

impl Incident {
    /// What two runs are compared on: the job, and for a failure its root
    /// cause. An alert's name isn't compared, because rules about the same
    /// symptom (backpressure, slow checkpoints) fire seconds apart, in
    /// either order.
    pub fn signature(&self) -> String {
        match self.kind.as_str() {
            "failure" => format!("{} failure: {}", self.job_name, self.detail),
            _ => format!("{} {}", self.job_name, self.kind),
        }
    }

    pub fn describe(&self) -> String {
        format!("{} {}: {}", self.job_name, self.kind, self.detail)
    }
}

const QUERY: &str = "
WITH symptoms AS (
    SELECT ts, job_name, 'failure' AS kind,
           regexp_replace(coalesce(root_cause_class, exception_class), '^.*\\.', '') AS detail
    FROM job_status
    WHERE ts >= $1 AND exception_class IS NOT NULL AND (job_name = ANY($2) OR job_name = ANY($3))
  UNION ALL
    SELECT ts, job_name, 'alert', alertname
    FROM alerts
    WHERE ts >= $1 AND status = 'firing' AND job_name = ANY($2)
)
SELECT DISTINCT ON (job_name) ts, job_name, kind, detail
FROM symptoms
ORDER BY job_name, ts, kind DESC";

/// `jobs` are the real jobs; `fleet` the fleet's genuine problems.
pub async fn since(
    database_url: &str,
    since: DateTime<Utc>,
    jobs: &[String],
    fleet: &[String],
) -> anyhow::Result<Vec<Incident>> {
    let mut db = PgConnection::connect(database_url).await?;
    let rows: Vec<(DateTime<Utc>, String, String, String)> =
        sqlx::query_as(QUERY).bind(since).bind(jobs).bind(fleet).fetch_all(&mut db).await?;
    let mut incidents: Vec<Incident> =
        rows.into_iter().map(|(ts, job_name, kind, detail)| Incident { ts, job_name, kind, detail }).collect();
    incidents.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.job_name.cmp(&b.job_name)));
    Ok(incidents)
}
