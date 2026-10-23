//! Gather: what code knows about an incident before Jev decides, and the
//! before-and-after health summaries it verifies against (docs/design.md
//! §8.3). As in triage, code computes the facts and Jev only judges them.
//!
//! The decision starts from the evidence triage gathered for the signal (in
//! its `judgment.state`) and adds what remediation needs: the job's state
//! now, its latest deploy and how soon the failures followed it, how long the
//! previous version ran healthily, and the job's capacity.

use chrono::{DateTime, Utc};
use platform_clients::{
    cicd::{CatalogueJob, CicdClient},
    flink::{FlinkClient, job_parallelism},
    ops_db::OpsDb,
    prometheus::{PrometheusClient, job_label},
};
use platform_model::{
    CheckpointStatus, Deployment, DeploymentKind, DeploymentOutcome, FailureClass, JobRecord, JobState, Signal, Tier,
};
use serde::Serialize;
use serde_json::{Value, json};

pub struct Sources {
    pub db: OpsDb,
    pub prometheus: PrometheusClient,
    pub flink: FlinkClient,
    pub cicd: CicdClient,
}

/// Everything the decision and the guardrails see.
pub struct Situation {
    /// The state Jev decides on.
    pub state: Value,
    pub job: Option<JobRecord>,
    /// The job's entry in the platform's catalogue: only jobs the platform's
    /// pipeline deploys can be rolled back, restarted or rescaled by it.
    pub catalogue: Option<CatalogueJob>,
    pub tier: Option<Tier>,
    pub class: Option<FailureClass>,
    pub class_probability: f64,
    pub latest_deploy: Option<DeployFacts>,
    pub previous_version: Option<PreviousVersion>,
    pub capacity: Option<Capacity>,
    /// The Flink job ID of the running job, if the cluster runs it.
    pub flink_job_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeployFacts {
    pub kind: &'static str,
    pub from_version: Option<String>,
    pub to_version: String,
    pub minutes_ago: f64,
    pub author: String,
    pub change_summary: String,
    /// When the job first failed after the deploy, if it has.
    pub first_failure_minutes_after: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreviousVersion {
    pub version: String,
    /// How long it ran before the latest deploy replaced it.
    pub ran_minutes: f64,
    /// The longest it ran, up to the deploy, without failing.
    pub ran_without_failure_minutes: f64,
    pub failures: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Capacity {
    pub parallelism: u32,
    /// The most the tenant's policy allows.
    pub max_parallelism: u32,
    pub free_slots: u32,
}

/// A job's health over a window, as of its end.
#[derive(Debug, Clone, Serialize)]
pub struct Health {
    pub window_minutes: f64,
    pub status: Option<String>,
    pub status_for_minutes: Option<f64>,
    pub failures: usize,
    /// The latest failure's root cause, if the job failed in the window.
    pub latest_failure: Option<String>,
    pub checkpoints_completed: usize,
    pub checkpoints_failed: usize,
    pub checkpoint_duration_seconds: Option<f64>,
    pub state_size_mb: Option<f64>,
    /// Records waiting in the job's input topics.
    pub lag_records: Option<f64>,
    /// How lag changed over the last minute, per minute.
    pub lag_change_per_minute: Option<f64>,
    pub backpressured_ms_per_second: Option<f64>,
    pub busy_ms_per_second: Option<f64>,
    pub records_in_per_second: Option<f64>,
}

pub fn minutes(from: DateTime<Utc>, to: DateTime<Utc>) -> f64 {
    ((to - from).num_milliseconds() as f64 / 60_000.0 * 10.0).round() / 10.0
}

fn round(x: f64, places: i32) -> f64 {
    let f = 10f64.powi(places);
    (x * f).round() / f
}

fn short_class(class: &str) -> &str {
    class.rsplit('.').next().unwrap_or(class)
}

/// Triage's evidence, without what only triage needs (its policy, the open
/// incidents it could attach to, its own history).
fn triage_evidence(signal: &Signal) -> Value {
    let mut state = signal.judgment.get("state").cloned().unwrap_or_else(|| json!({}));
    if let Some(map) = state.as_object_mut() {
        for key in ["triage_policy", "open_incidents", "previous_triage", "alert_history", "job"] {
            map.remove(key);
        }
    }
    state
}

pub fn class_probability(signal: &Signal) -> f64 {
    let class = signal.failure_class.and_then(|c| serde_json::to_value(c).ok());
    let Some(Value::String(class)) = class else { return 0.0 };
    signal.judgment["answers"]["failure_class"]["probabilities"][&class].as_f64().unwrap_or(0.0)
}

impl Sources {
    /// Gathers the situation for a raised signal, as of `at`.
    pub async fn situation(&self, context: &Value, signal: &Signal, at: DateTime<Utc>) -> anyhow::Result<Situation> {
        let job_name = &signal.job_name;
        let job = self.db.job(job_name).await?;
        let catalogue = self.cicd.catalogue().await?.jobs.remove(job_name);
        let deployments = self.db.deployments_before(job_name, at, 10).await?;
        let statuses = self.db.status_between(job_name, at - chrono::Duration::hours(3), at).await?;
        let latest = deployments.iter().find(|d| d.outcome == DeploymentOutcome::Succeeded);
        let latest_deploy = latest.map(|d| deploy_facts(d, &statuses, at));
        let previous_version = latest.and_then(|d| previous_version(d, &deployments, &statuses));
        let flink_job = match &catalogue {
            Some(_) => self.flink.active_job(job_name).await?,
            None => None,
        };
        let capacity = match (&flink_job, &catalogue) {
            (Some(run), Some(spec)) => {
                let parallelism = job_parallelism(&self.flink.vertices(&run.jid).await?);
                let overview = self.flink.overview().await?;
                Some(Capacity {
                    parallelism,
                    max_parallelism: spec.max_parallelism,
                    free_slots: overview.slots_available,
                })
            }
            _ => None,
        };
        let now = self.health(job_name, at - chrono::Duration::minutes(5), at).await?;
        let tier = job.as_ref().map(|j| j.tier).or(catalogue.as_ref().map(|c| c.tier));
        let class_probability = class_probability(signal);
        let state = json!({
            "remediation_policy": context,
            "incident": {
                "job": {
                    "name": job_name,
                    "tenant": signal.tenant,
                    "owner": job.as_ref().map(|j| &j.owner),
                    "tier": tier,
                    "version": job.as_ref().map(|j| &j.current_version),
                },
                "triage": {
                    "failure_class": signal.failure_class,
                    "failure_class_probability": round(class_probability, 2),
                    "actionable": signal.actionable.map(|p| round(p, 2)),
                    "user_impact": signal.user_impact.map(|p| round(p, 2)),
                    "deploy_related": signal.deploy_related.map(|p| round(p, 2)),
                    "summary": signal.judgment.get("summary"),
                    "minutes_ago": minutes(signal.ts, at),
                },
            },
            "evidence": triage_evidence(signal),
            "now": now,
            "latest_deploy": latest_deploy,
            "previous_version": previous_version,
            "capacity": capacity,
        });
        Ok(Situation {
            state,
            job,
            catalogue,
            tier,
            class: signal.failure_class,
            class_probability,
            latest_deploy,
            previous_version,
            capacity,
            flink_job_id: flink_job.map(|j| j.jid),
        })
    }

    /// The job's health over `[from, to]`, as of `to`. A metric query that
    /// fails leaves its fact out.
    pub async fn health(&self, job_name: &str, from: DateTime<Utc>, to: DateTime<Utc>) -> anyhow::Result<Health> {
        let statuses = self.db.status_between(job_name, from, to).await?;
        let snapshot = self.db.job_at(job_name, to).await?;
        let checkpoints: Vec<_> =
            self.db.checkpoints_between(job_name, from, to).await?.into_iter().filter(|c| !c.is_savepoint).collect();
        let completed: Vec<_> = checkpoints.iter().filter(|c| c.status == CheckpointStatus::Completed).collect();
        let mut durations: Vec<f64> = completed.iter().map(|c| c.duration_ms as f64 / 1000.0).collect();
        durations.sort_by(f64::total_cmp);
        let latest_failure = statuses
            .iter()
            .rev()
            .find_map(|s| s.failure.as_ref())
            .map(|f| short_class(f.root_cause_class.as_deref().unwrap_or(&f.exception_class)).to_string());

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
        let lag_now = q(lag.clone(), to).await;
        let lag_before = q(lag, to - chrono::Duration::minutes(1)).await;
        Ok(Health {
            window_minutes: minutes(from, to),
            status: snapshot.status.as_ref().map(|s| s.status.as_str().to_string()),
            status_for_minutes: snapshot.status.as_ref().map(|s| minutes(s.ts, to)),
            failures: statuses.iter().filter(|s| s.failure.is_some()).count(),
            latest_failure,
            checkpoints_completed: completed.len(),
            checkpoints_failed: checkpoints.len() - completed.len(),
            checkpoint_duration_seconds: durations.get(durations.len() / 2).map(|s| round(*s, 1)),
            state_size_mb: completed.last().map(|c| round(c.state_size_bytes as f64 / 1e6, 1)),
            lag_records: lag_now,
            lag_change_per_minute: lag_now.zip(lag_before).map(|(now, before)| round(now - before, 0)),
            backpressured_ms_per_second: q(
                format!(r#"max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond{{job_name="{label}"}})"#),
                to,
            )
            .await,
            busy_ms_per_second: q(
                format!(r#"max(flink_taskmanager_job_task_busyTimeMsPerSecond{{job_name="{label}"}})"#),
                to,
            )
            .await,
            records_in_per_second: q(
                format!(r#"sum(flink_taskmanager_job_task_numRecordsInPerSecond{{job_name="{label}"}})"#),
                to,
            )
            .await,
        })
    }

    /// Automatic actions the operator took on the job since `since`: the
    /// ones its guardrails approved.
    pub async fn automatic_actions(&self, job_name: &str, since: DateTime<Utc>) -> anyhow::Result<usize> {
        let actions = self.db.actions_between(job_name, since, Utc::now() + chrono::Duration::seconds(1)).await?;
        Ok(actions.iter().filter(|a| a.actor == GUARDRAILS && a.phase == platform_model::ActionPhase::Approved).count())
    }

    /// The running job's parallelism, if the cluster runs it.
    pub async fn parallelism(&self, job_name: &str) -> anyhow::Result<Option<(String, u32)>> {
        let Some(run) = self.flink.active_job(job_name).await? else { return Ok(None) };
        if run.state != JobState::Running {
            return Ok(None);
        }
        let parallelism = job_parallelism(&self.flink.vertices(&run.jid).await?);
        Ok(Some((run.jid, parallelism)))
    }
}

/// The actor that approves automatic actions in `platform.actions`.
pub const GUARDRAILS: &str = "operator:guardrails";

fn deploy_facts(d: &Deployment, statuses: &[platform_model::JobStatusEvent], at: DateTime<Utc>) -> DeployFacts {
    let first_failure = statuses.iter().find(|s| s.ts > d.ts && s.failure.is_some());
    DeployFacts {
        kind: match d.kind {
            DeploymentKind::Deploy => "deploy",
            DeploymentKind::Rollback => "rollback",
        },
        from_version: d.from_version.clone(),
        to_version: d.to_version.clone(),
        minutes_ago: minutes(d.ts, at),
        author: d.author.clone(),
        change_summary: d.summary.clone(),
        first_failure_minutes_after: first_failure.map(|s| minutes(d.ts, s.ts)),
    }
}

/// How the version before `latest` fared until `latest` replaced it, from
/// the deployment that started it.
fn previous_version(
    latest: &Deployment,
    deployments: &[Deployment],
    statuses: &[platform_model::JobStatusEvent],
) -> Option<PreviousVersion> {
    let version = latest.from_version.clone()?;
    let started = deployments
        .iter()
        .find(|d| d.ts < latest.ts && d.outcome == DeploymentOutcome::Succeeded && d.to_version == version)?;
    let failures: Vec<_> =
        statuses.iter().filter(|s| s.ts >= started.ts && s.ts < latest.ts && s.failure.is_some()).collect();
    // The longest stretch without a failure, between the deploys.
    let mut marks: Vec<DateTime<Utc>> = vec![started.ts];
    marks.extend(failures.iter().map(|s| s.ts));
    marks.push(latest.ts);
    let longest = marks.windows(2).map(|w| minutes(w[0], w[1])).fold(0.0, f64::max);
    Some(PreviousVersion {
        version,
        ran_minutes: minutes(started.ts, latest.ts),
        ran_without_failure_minutes: longest,
        failures: failures.len(),
    })
}

/// The state for `recovered`: the action, what recovery looks like after
/// it, and the job's health before and after.
pub fn verify_state(
    job_name: &str,
    action: &str,
    expected: &str,
    params: &Value,
    minutes_ago: f64,
    before: &Health,
    after: &Health,
) -> Value {
    json!({
        "job": job_name,
        "action": {"taken": action, "params": params, "minutes_ago": minutes_ago, "expected": expected},
        "before": before,
        "after": after,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_model::{Failure, JobStatusEvent};
    use uuid::Uuid;

    fn t(minute: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + minute * 60, 0).unwrap()
    }

    fn deployment(minute: i64, from: Option<&str>, to: &str) -> Deployment {
        Deployment {
            event_id: Uuid::new_v4(),
            ts: t(minute),
            job_name: "payments-enrichment".into(),
            tenant: "payments-eng".into(),
            kind: DeploymentKind::Deploy,
            from_version: from.map(str::to_string),
            to_version: to.into(),
            commit_sha: "abc".into(),
            author: "someone".into(),
            summary: "a change".into(),
            args: vec![],
            triggered_by: "director".into(),
            outcome: DeploymentOutcome::Succeeded,
            failure_reason: None,
            flink_job_id: None,
            restored_from: None,
        }
    }

    fn failure(minute: i64) -> JobStatusEvent {
        JobStatusEvent {
            event_id: Uuid::new_v4(),
            ts: t(minute),
            job_name: "payments-enrichment".into(),
            flink_job_id: "j".into(),
            status: JobState::Restarting,
            previous_status: Some(JobState::Running),
            restarts: 1,
            failure: Some(Failure {
                exception_class: "java.lang.NullPointerException".into(),
                message: None,
                root_cause_class: None,
                root_cause_message: None,
                stack_head: None,
                task: None,
            }),
        }
    }

    #[test]
    fn the_previous_versions_longest_healthy_stretch_and_the_first_failure_after_a_deploy() {
        let latest = deployment(30, Some("1.41.0"), "1.42.0");
        let deployments = vec![latest.clone(), deployment(0, None, "1.41.0")];
        let statuses = vec![failure(5), failure(31)];
        let previous = previous_version(&latest, &deployments, &statuses).unwrap();
        assert_eq!(previous.version, "1.41.0");
        assert_eq!(previous.ran_minutes, 30.0);
        assert_eq!(previous.ran_without_failure_minutes, 25.0);
        assert_eq!(previous.failures, 1);
        let facts = deploy_facts(&latest, &statuses, t(40));
        assert_eq!(facts.first_failure_minutes_after, Some(1.0));
        assert_eq!(facts.minutes_ago, 10.0);
    }
}
