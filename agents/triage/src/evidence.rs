//! Evidence: what code gathers about a job before Jev judges it (docs/design.md
//! §6.1). Metric summaries, failure digests and history counts are computed
//! here, as of a point in time; Jev sees only the result, as the request's
//! `state`. Times are relative ("minutes ago"), so the state reads the same
//! however long ago it was gathered.
//!
//! A deployment's job arguments are left out: they carry the fault flags
//! (`--fault=…`), which an operator's evidence wouldn't.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use platform_clients::{
    ops_db::OpsDb,
    prometheus::{PrometheusClient, job_label},
};
use platform_model::{
    AlertEvent, AlertStatus, CheckpointStatus, Deployment, DeploymentKind, DeploymentOutcome, Disposition, JobRecord,
    LogLevel, Tier,
};
use serde::Serialize;

use crate::{
    config::{Config, KnownNoise},
    incidents::Incident,
};

pub struct Sources {
    pub db: OpsDb,
    pub prometheus: PrometheusClient,
}

/// The evidence about one group of alerts, with what code needs alongside it.
pub struct Gathered {
    pub evidence: Evidence,
    pub tenant: String,
    /// The latest deployment, if it's in the window.
    pub deployment: Option<Deployment>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Evidence {
    /// What the platform team knows about its alerts (`context` in the
    /// config).
    pub triage_policy: serde_json::Value,
    pub job: JobFacts,
    /// The platform team's note, if they triaged this job's alerts as known
    /// noise long ago.
    pub known_noise: Option<KnownNoise>,
    /// The alerts being judged.
    pub alerts: Vec<AlertFacts>,
    pub alert_history: AlertHistory,
    pub status: StatusFacts,
    /// The job's failures in the window, grouped by root cause.
    pub failures: Vec<FailureFacts>,
    /// The most frequent warnings and errors in the job's logs.
    pub log_messages: Vec<LogFacts>,
    pub checkpoints: CheckpointFacts,
    pub metrics: MetricFacts,
    /// Recent deployments, newest first.
    pub deployments: Vec<DeploymentFacts>,
    pub previous_triage: TriageHistory,
    /// Incidents still open for this job or its tenant.
    pub open_incidents: Vec<IncidentFacts>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobFacts {
    pub name: String,
    pub tenant: String,
    pub owner: Option<String>,
    pub tier: Option<&'static str>,
    pub kind: Option<&'static str>,
    pub version: Option<String>,
    pub version_deployed_minutes_ago: Option<f64>,
    pub in_production_days: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AlertFacts {
    pub alert: String,
    pub description: Option<String>,
    pub firing_for_seconds: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AlertHistory {
    pub window_minutes: i64,
    /// How often each alert fired for this job in the window.
    pub fired: BTreeMap<String, usize>,
    /// Alerts in the window that resolved within 5 minutes of firing.
    pub resolved_within_5_minutes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusFacts {
    pub current: Option<String>,
    /// The job failed in the window, and since its latest failure it has
    /// been RUNNING without failing again for at least `recovery`.
    pub recovered_from_latest_failure: Option<bool>,
    pub current_for_minutes: Option<f64>,
    pub failures_in_window: usize,
    /// Times the job got back to RUNNING after a failure in the window.
    pub recoveries_in_window: usize,
    pub last_failure_minutes_ago: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailureFacts {
    /// The root cause's class (the innermost `Caused by:`).
    pub exception: String,
    /// The top-level exception, when it differs.
    pub wrapped_in: Option<String>,
    pub message: Option<String>,
    pub count: usize,
    pub first_minutes_ago: f64,
    pub last_minutes_ago: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogFacts {
    pub level: &'static str,
    pub message: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointFacts {
    pub completed_in_window: usize,
    /// Completed checkpoints since the job's latest failure in the window.
    pub completed_since_last_failure: Option<usize>,
    pub failed_in_window: usize,
    pub failure_reasons: Vec<String>,
    pub last_completed_minutes_ago: Option<f64>,
    /// Median duration of the last 5 minutes' completed checkpoints.
    pub recent_duration_seconds: Option<f64>,
    /// The slowest of the last 5 minutes' completed checkpoints.
    pub recent_max_duration_seconds: Option<f64>,
    /// Median duration of the window's earlier checkpoints, as a baseline.
    pub baseline_duration_seconds: Option<f64>,
    pub state_size_mb: Option<f64>,
    pub state_size_mb_at_window_start: Option<f64>,
    pub state_growth_mb_per_minute: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricFacts {
    /// Records waiting in the job's input topics.
    pub lag_records_now: Option<f64>,
    pub lag_records_5_minutes_ago: Option<f64>,
    pub lag_records_15_minutes_ago: Option<f64>,
    /// The worst task's backpressure, in ms per second (0–1000).
    pub backpressured_ms_per_second: Option<f64>,
    /// The busiest task's busy time, in ms per second (0–1000).
    pub busy_ms_per_second: Option<f64>,
    pub records_in_per_second_now: Option<f64>,
    pub records_in_per_second_15_minutes_ago: Option<f64>,
    /// Input rate now as a multiple of 15 minutes ago.
    pub input_rate_vs_15_minutes_ago: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeploymentFacts {
    pub kind: &'static str,
    pub from_version: Option<String>,
    pub to_version: String,
    pub minutes_ago: f64,
    pub author: String,
    pub change_summary: String,
    pub triggered_by: String,
    pub succeeded: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TriageHistory {
    pub raised: usize,
    pub attached: usize,
    pub suppressed: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct IncidentFacts {
    /// The option name `same_incident` uses for it.
    pub id: String,
    pub job: String,
    pub tenant: String,
    pub failure_class: Option<String>,
    pub opened_minutes_ago: f64,
    pub summary: String,
}

fn minutes(from: DateTime<Utc>, to: DateTime<Utc>) -> f64 {
    ((to - from).num_milliseconds() as f64 / 60_000.0 * 10.0).round() / 10.0
}

fn round(x: f64, places: i32) -> f64 {
    let f = 10f64.powi(places);
    (x * f).round() / f
}

fn median(mut xs: Vec<f64>) -> Option<f64> {
    if xs.is_empty() {
        return None;
    }
    xs.sort_by(f64::total_cmp);
    Some(xs[xs.len() / 2])
}

fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

/// The class name without its package, e.g. `NullPointerException`.
pub fn short_class(class: &str) -> &str {
    class.rsplit('.').next().unwrap_or(class)
}

/// Groups messages that differ only in numbers (offsets, IDs, durations).
fn shape(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    for c in message.chars() {
        if !c.is_ascii_digit() {
            out.push(c);
        } else if !out.ends_with('#') {
            out.push('#');
        }
    }
    out
}

fn tier(t: Tier) -> &'static str {
    match t {
        Tier::Tier1 => "tier1 (business-critical: pages someone)",
        Tier::Tier2 => "tier2",
        Tier::Tier3 => "tier3 (best effort)",
    }
}

impl Sources {
    /// Gathers the evidence about `alerts` (one job's group) as of `at`.
    /// `earlier` adds signals the ops DB doesn't have (a replay's own). The
    /// open incidents are added with [`Evidence::with_open_incidents`], once
    /// the tenant is known.
    pub async fn gather(
        &self,
        config: &Config,
        job_name: &str,
        alerts: &[AlertEvent],
        earlier: &[platform_model::Signal],
        at: DateTime<Utc>,
    ) -> anyhow::Result<Gathered> {
        let since = at - config.history;
        let registry = self.db.job(job_name).await?;
        let tenant = registry
            .as_ref()
            .map(|j| j.tenant.clone())
            .or_else(|| alerts.iter().find_map(|a| a.tenant.clone()))
            .unwrap_or_default();

        let deployments: Vec<Deployment> = self
            .db
            .deployments_before(job_name, at, 5)
            .await?
            .into_iter()
            .filter(|d| (at - d.ts).to_std().is_ok_and(|age| age <= config.deploy_window))
            .collect();
        let history = self.db.alerts_between(Some(job_name), since, at).await?;
        let statuses = self.db.status_between(job_name, since, at).await?;
        let snapshot = self.db.job_at(job_name, at).await?;
        let logs = self.db.logs_between(job_name, since, at, LogLevel::Warn, 500).await?;
        let checkpoints = self.db.checkpoints_between(job_name, since, at).await?;
        let mut signals = self.db.signals_between(Some(job_name), since, at).await?;
        signals.extend(earlier.iter().filter(|s| s.job_name == job_name && s.ts >= since && s.ts < at).cloned());

        let last_failure = statuses.iter().rev().find(|s| s.failure.is_some());
        let checkpoints_facts = checkpoint_facts(&checkpoints, last_failure, at);
        let mut status = status_facts(&statuses, snapshot.status.as_ref(), at);
        if let Some(failure) = last_failure {
            let after: Vec<_> = statuses.iter().filter(|s| s.ts > failure.ts).cloned().collect();
            status.recovered_from_latest_failure = Some(recovered_at(&after, config).is_some_and(|t| t <= at));
        }
        let evidence = Evidence {
            triage_policy: config.context.clone(),
            job: job_facts(job_name, &tenant, registry.as_ref(), snapshot.deployment.as_ref(), at),
            known_noise: config.notes.get(job_name).cloned(),
            alerts: alerts
                .iter()
                .map(|a| AlertFacts {
                    alert: a.alertname.clone(),
                    description: a.annotations.get("description").or_else(|| a.annotations.get("summary")).cloned(),
                    firing_for_seconds: (at - a.starts_at).num_seconds().max(0),
                })
                .collect(),
            alert_history: alert_history(&history, config),
            status,
            failures: failure_facts(&statuses, at),
            log_messages: log_facts(&logs),
            checkpoints: checkpoints_facts,
            metrics: self.metric_facts(job_name, at).await,
            deployments: deployments.iter().map(|d| deployment_facts(d, at)).collect(),
            previous_triage: TriageHistory {
                raised: signals.iter().filter(|s| s.disposition == Disposition::Raise).count(),
                attached: signals.iter().filter(|s| s.disposition == Disposition::Attach).count(),
                suppressed: signals.iter().filter(|s| s.disposition == Disposition::Suppress).count(),
            },
            open_incidents: Vec::new(),
        };
        Ok(Gathered { evidence, tenant, deployment: deployments.into_iter().next() })
    }

    /// When a group due at `due` can be judged, as far as is known at `now`:
    /// if the job failed less than `settle` before `due`, once it has
    /// recovered (RUNNING for `recovery` without failing again), or `settle`
    /// after the failure if it hasn't. A job that failed three times or more
    /// in that time is in a restart loop and is judged at once. `None` means
    /// not yet.
    pub async fn settled(
        &self,
        config: &Config,
        job_name: &str,
        due: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        let recent = self.db.status_between(job_name, due - config.settle, due + chrono::Duration::seconds(1)).await?;
        let Some(failure) = recent.iter().rev().find(|s| s.failure.is_some()) else { return Ok(Some(due)) };
        // Failing again and again isn't a transient restart: no need to wait.
        if recent.iter().filter(|s| s.failure.is_some()).count() >= 3 {
            return Ok(Some(due));
        }
        let deadline = failure.ts + config.settle;
        let horizon = deadline.min(now);
        let after = self.db.status_between(job_name, failure.ts + chrono::Duration::milliseconds(1), horizon).await?;
        if let Some(recovered) = recovered_at(&after, config)
            && recovered <= horizon
        {
            return Ok(Some(due.max(recovered)));
        }
        Ok((now >= deadline).then_some(deadline.max(due)))
    }

    /// Metric summaries from Prometheus. A query that fails leaves its fact
    /// out rather than failing the judgment.
    async fn metric_facts(&self, job_name: &str, at: DateTime<Utc>) -> MetricFacts {
        let label = job_label(job_name);
        let q = |promql: String, at: DateTime<Utc>| async move {
            match self.prometheus.scalar(&promql, at).await {
                Ok(v) => v.map(|v| round(v, 1)),
                Err(err) => {
                    tracing::warn!(%err, promql, "metric query");
                    None
                }
            }
        };
        let lag = format!(r#"sum(flink_taskmanager_job_task_operator_pendingRecords{{job_name="{label}"}})"#);
        let rate = format!(r#"sum(flink_taskmanager_job_task_numRecordsInPerSecond{{job_name="{label}"}})"#);
        let minutes_ago = |m: i64| at - chrono::Duration::minutes(m);
        let (rate_now, rate_before) = (q(rate.clone(), at).await, q(rate, minutes_ago(15)).await);
        MetricFacts {
            lag_records_now: q(lag.clone(), at).await,
            lag_records_5_minutes_ago: q(lag.clone(), minutes_ago(5)).await,
            lag_records_15_minutes_ago: q(lag, minutes_ago(15)).await,
            backpressured_ms_per_second: q(
                format!(r#"max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond{{job_name="{label}"}})"#),
                at,
            )
            .await,
            busy_ms_per_second: q(
                format!(r#"max(flink_taskmanager_job_task_busyTimeMsPerSecond{{job_name="{label}"}})"#),
                at,
            )
            .await,
            records_in_per_second_now: rate_now,
            records_in_per_second_15_minutes_ago: rate_before,
            input_rate_vs_15_minutes_ago: match (rate_now, rate_before) {
                (Some(now), Some(before)) if before >= 1.0 => Some(round(now / before, 1)),
                _ => None,
            },
        }
    }
}

impl Evidence {
    pub fn with_open_incidents(mut self, incidents: &[Incident], at: DateTime<Utc>) -> Self {
        self.open_incidents = incidents
            .iter()
            .map(|i| IncidentFacts {
                id: i.key(),
                job: i.job_name.clone(),
                tenant: i.tenant.clone(),
                failure_class: i.class.map(|c| crate::questions::class_name(c).to_string()),
                opened_minutes_ago: minutes(i.opened_at, at),
                summary: i.summary.clone(),
            })
            .collect();
        self
    }
}

/// The registry is current state; the version as of `at` comes from the
/// latest deployment by then, when there is one.
fn job_facts(
    job_name: &str,
    tenant: &str,
    job: Option<&JobRecord>,
    deployment: Option<&Deployment>,
    at: DateTime<Utc>,
) -> JobFacts {
    let (version, deployed_at) = match (deployment, job) {
        (Some(d), _) => (Some(d.to_version.clone()), Some(d.ts)),
        (None, Some(j)) if j.deployed_at <= at => (Some(j.current_version.clone()), Some(j.deployed_at)),
        _ => (None, None),
    };
    JobFacts {
        name: job_name.to_string(),
        tenant: tenant.to_string(),
        owner: job.map(|j| j.owner.clone()),
        tier: job.map(|j| tier(j.tier)),
        kind: job.map(|j| match j.kind {
            platform_model::JobKind::Java => "java",
            platform_model::JobKind::Sql => "sql",
        }),
        version,
        version_deployed_minutes_ago: deployed_at.map(|t| minutes(t, at)),
        in_production_days: job.map(|j| round((at - j.first_deployed_at).num_hours() as f64 / 24.0, 1)),
    }
}

/// When the job had been RUNNING for `recovery` with no failure since, given
/// its status events after a failure, oldest first.
fn recovered_at(after: &[platform_model::JobStatusEvent], config: &Config) -> Option<DateTime<Utc>> {
    let last_failure = after.iter().rposition(|s| s.failure.is_some());
    let since = match last_failure {
        Some(i) => &after[i + 1..],
        None => after,
    };
    let running = since.iter().find(|s| s.status == platform_model::JobState::Running)?;
    let later_trouble = since.iter().any(|s| s.ts > running.ts && s.status != platform_model::JobState::Running);
    (!later_trouble).then(|| running.ts + config.recovery)
}

fn alert_history(history: &[AlertEvent], config: &Config) -> AlertHistory {
    let mut fired = BTreeMap::new();
    let mut resolved_quickly = 0;
    for a in history {
        match a.status {
            AlertStatus::Firing => *fired.entry(a.alertname.clone()).or_insert(0) += 1,
            AlertStatus::Resolved if (a.ts - a.starts_at).num_seconds() <= 300 => resolved_quickly += 1,
            AlertStatus::Resolved => {}
        }
    }
    AlertHistory {
        window_minutes: config.history.as_secs() as i64 / 60,
        fired,
        resolved_within_5_minutes: resolved_quickly,
    }
}

fn status_facts(
    statuses: &[platform_model::JobStatusEvent],
    latest: Option<&platform_model::JobStatusEvent>,
    at: DateTime<Utc>,
) -> StatusFacts {
    let mut recoveries = 0;
    let mut failed = false;
    for s in statuses {
        if s.failure.is_some() {
            failed = true;
        } else if failed && s.status == platform_model::JobState::Running {
            recoveries += 1;
            failed = false;
        }
    }
    StatusFacts {
        current: latest.map(|s| s.status.as_str().to_string()),
        recovered_from_latest_failure: None,
        current_for_minutes: latest.map(|s| minutes(s.ts, at)),
        failures_in_window: statuses.iter().filter(|s| s.failure.is_some()).count(),
        recoveries_in_window: recoveries,
        last_failure_minutes_ago: statuses.iter().rev().find(|s| s.failure.is_some()).map(|s| minutes(s.ts, at)),
    }
}

fn failure_facts(statuses: &[platform_model::JobStatusEvent], at: DateTime<Utc>) -> Vec<FailureFacts> {
    let mut groups: BTreeMap<String, FailureFacts> = BTreeMap::new();
    for s in statuses {
        let Some(f) = &s.failure else { continue };
        let root = f.root_cause_class.clone().unwrap_or_else(|| f.exception_class.clone());
        let ago = minutes(s.ts, at);
        let entry = groups.entry(root.clone()).or_insert_with(|| FailureFacts {
            wrapped_in: (f.exception_class != root).then(|| f.exception_class.clone()),
            exception: root,
            message: f.root_cause_message.clone().or_else(|| f.message.clone()).map(|m| truncate(&m, 240)),
            count: 0,
            first_minutes_ago: ago,
            last_minutes_ago: ago,
        });
        entry.count += 1;
        entry.last_minutes_ago = ago;
    }
    let mut facts: Vec<_> = groups.into_values().collect();
    facts.sort_by_key(|f| std::cmp::Reverse(f.count));
    facts.truncate(3);
    facts
}

fn log_facts(logs: &[platform_model::LogEvent]) -> Vec<LogFacts> {
    let mut groups: BTreeMap<(LogLevel, String), LogFacts> = BTreeMap::new();
    for l in logs {
        let text = match (&l.exception_class, &l.exception_message) {
            (Some(class), Some(message)) => format!("{}: {message}", short_class(class)),
            (Some(class), None) => format!("{}: {}", short_class(class), l.message),
            _ => l.message.clone(),
        };
        let entry = groups.entry((l.level, shape(&text))).or_insert_with(|| LogFacts {
            level: l.level.as_str(),
            message: truncate(&text, 200),
            count: 0,
        });
        entry.count += 1;
    }
    let mut facts: Vec<_> = groups.into_values().collect();
    facts.sort_by_key(|f| std::cmp::Reverse(f.count));
    facts.truncate(3);
    facts
}

fn checkpoint_facts(
    checkpoints: &[platform_model::CheckpointEvent],
    last_failure: Option<&platform_model::JobStatusEvent>,
    at: DateTime<Utc>,
) -> CheckpointFacts {
    let regular: Vec<_> = checkpoints.iter().filter(|c| !c.is_savepoint).collect();
    let completed: Vec<_> = regular.iter().filter(|c| c.status == CheckpointStatus::Completed).copied().collect();
    let failed: Vec<_> = regular.iter().filter(|c| c.status == CheckpointStatus::Failed).copied().collect();
    let mut reasons: Vec<String> = Vec::new();
    for c in &failed {
        if let Some(r) = &c.failure_reason {
            let r = truncate(r, 160);
            if !reasons.contains(&r) {
                reasons.push(r);
            }
        }
    }
    reasons.truncate(3);
    let seconds = |c: &&platform_model::CheckpointEvent| c.duration_ms as f64 / 1000.0;
    let recent_from = at - chrono::Duration::minutes(5);
    let baseline_to = at - chrono::Duration::minutes(10);
    let mb = |c: &platform_model::CheckpointEvent| round(c.state_size_bytes as f64 / 1e6, 2);
    let (first, last) = (completed.first(), completed.last());
    let growth = match (first, last) {
        (Some(f), Some(l)) if (l.ts - f.ts).num_seconds() >= 120 => {
            Some(round((mb(l) - mb(f)) / minutes(f.ts, l.ts), 3))
        }
        _ => None,
    };
    CheckpointFacts {
        completed_in_window: completed.len(),
        completed_since_last_failure: last_failure.map(|f| completed.iter().filter(|c| c.ts > f.ts).count()),
        failed_in_window: failed.len(),
        failure_reasons: reasons,
        last_completed_minutes_ago: last.map(|c| minutes(c.ts, at)),
        recent_duration_seconds: median(completed.iter().filter(|c| c.ts >= recent_from).map(seconds).collect())
            .map(|s| round(s, 1)),
        recent_max_duration_seconds: completed
            .iter()
            .filter(|c| c.ts >= recent_from)
            .map(seconds)
            .max_by(f64::total_cmp)
            .map(|s| round(s, 1)),
        baseline_duration_seconds: median(completed.iter().filter(|c| c.ts < baseline_to).map(seconds).collect())
            .map(|s| round(s, 1)),
        state_size_mb: last.map(|c| mb(c)),
        state_size_mb_at_window_start: first.map(|c| mb(c)),
        state_growth_mb_per_minute: growth,
    }
}

fn deployment_facts(d: &Deployment, at: DateTime<Utc>) -> DeploymentFacts {
    DeploymentFacts {
        kind: match d.kind {
            DeploymentKind::Deploy => "deploy",
            DeploymentKind::Rollback => "rollback",
        },
        from_version: d.from_version.clone(),
        to_version: d.to_version.clone(),
        minutes_ago: minutes(d.ts, at),
        author: d.author.clone(),
        change_summary: d.summary.clone(),
        triggered_by: d.triggered_by.clone(),
        succeeded: d.outcome == DeploymentOutcome::Succeeded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_that_differ_in_numbers_share_a_shape() {
        assert_eq!(shape("offset 42 in partition 3"), shape("offset 977 in partition 1"));
        assert_eq!(short_class("java.lang.NullPointerException"), "NullPointerException");
        assert_eq!(truncate("abcdef", 3), "abc…");
        assert_eq!(median(vec![3.0, 1.0, 2.0]), Some(2.0));
    }
}
