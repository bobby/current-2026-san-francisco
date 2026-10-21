//! The deploy pipeline. A deploy:
//!
//! 1. stops the running version: stop-with-savepoint if the job is running.
//!    If it can't take a savepoint (e.g. it is restart-looping), the run is
//!    cancelled and the next run restores from the newest retained
//!    checkpoint or savepoint in the ops DB;
//! 2. submits the requested version from that savepoint or checkpoint (a jar
//!    for Java jobs, a SQL script through the SQL Gateway);
//! 3. waits for Flink to start it, then publishes a `platform.deployments`
//!    event and the job's `platform.jobs` record.
//!
//! A rollback is a deploy of the previous version through the same pipeline.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use platform_clients::{
    flink::{FlinkClient, JobOverview, SavepointStatus},
    kafka::Publisher,
    ops_db::OpsDb,
    sql_gateway::{self, SqlGateway},
};
use platform_model::{
    ActionEvent, ActionKind, ActionPhase, Deployment, DeploymentKind, DeploymentOutcome, JobKind, JobRecord, JobState,
    topics,
};
use serde::Deserialize;
use tracing::{info, warn};
use uuid::Uuid;

use crate::catalogue::{Catalogue, JobSpec, Version};

const ENTRY_CLASS: &str = "demo.platform.jobs.Main";

#[derive(Debug, Clone, Deserialize)]
pub struct DeployRequest {
    pub job: String,
    pub version: String,
    /// Extra job arguments for this deploy (Java jobs), e.g.
    /// `--on-deser-error=dlq`.
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(flatten)]
    pub context: RequestContext,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RollbackRequest {
    pub job: String,
    /// Defaults to the version that ran before the current one.
    pub to_version: Option<String>,
    #[serde(flatten)]
    pub context: RequestContext,
}

/// Who asked, and the remediation it belongs to, if any.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RequestContext {
    #[serde(default = "unknown")]
    pub triggered_by: String,
    /// Set by agents and the chatops bot: the action this deploy carries
    /// out. cicd-sim then records its outcome in `platform.actions`.
    pub action_id: Option<Uuid>,
    pub incident_id: Option<Uuid>,
    pub signal_id: Option<Uuid>,
}

fn unknown() -> String {
    "unknown".into()
}

#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    #[error("no job named {0} in the version catalogue")]
    UnknownJob(String),
    #[error("{job} has no version {version} in the version catalogue")]
    UnknownVersion { job: String, version: String },
    #[error("{0} has no previous version to roll back to")]
    NoPreviousVersion(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

pub struct Deployer {
    pub flink: FlinkClient,
    pub gateway: SqlGateway,
    pub db: OpsDb,
    pub publisher: Publisher,
    pub catalogue_path: PathBuf,
    pub jar: PathBuf,
    pub sql_dir: PathBuf,
    pub savepoint_timeout: Duration,
    pub start_timeout: Duration,
    /// One deploy at a time per job.
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// What this process deployed, ahead of the ops DB (which fills from
    /// Kafka a moment later).
    registry: Mutex<HashMap<String, (JobRecord, Deployment)>>,
}

impl Deployer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        flink: FlinkClient,
        gateway: SqlGateway,
        db: OpsDb,
        publisher: Publisher,
        catalogue_path: PathBuf,
        jar: PathBuf,
        sql_dir: PathBuf,
        savepoint_timeout: Duration,
        start_timeout: Duration,
    ) -> Self {
        Self {
            flink,
            gateway,
            db,
            publisher,
            catalogue_path,
            jar,
            sql_dir,
            savepoint_timeout,
            start_timeout,
            locks: Default::default(),
            registry: Default::default(),
        }
    }

    /// The catalogue is re-read on every request, so edits apply without a restart.
    pub fn catalogue(&self) -> anyhow::Result<Catalogue> {
        Catalogue::load(&self.catalogue_path)
    }

    fn lock(&self, job: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks.lock().unwrap().entry(job.to_string()).or_default().clone()
    }

    pub async fn deploy(&self, request: DeployRequest) -> Result<Deployment, DeployError> {
        let catalogue = self.catalogue()?;
        let spec = catalogue.job(&request.job).ok_or_else(|| DeployError::UnknownJob(request.job.clone()))?;
        let version = spec.version(&request.version).ok_or_else(|| DeployError::UnknownVersion {
            job: request.job.clone(),
            version: request.version.clone(),
        })?;
        let lock = self.lock(&request.job);
        let _guard = lock.lock().await;
        self.run_pipeline(&request.job, spec, version, &request.args, DeploymentKind::Deploy, &request.context).await
    }

    pub async fn rollback(&self, request: RollbackRequest) -> Result<Deployment, DeployError> {
        let catalogue = self.catalogue()?;
        let spec = catalogue.job(&request.job).ok_or_else(|| DeployError::UnknownJob(request.job.clone()))?;
        let lock = self.lock(&request.job);
        let _guard = lock.lock().await;
        let target = match request.to_version {
            Some(version) => version,
            None => self.previous_version(&request.job).await?,
        };
        let version = spec
            .version(&target)
            .ok_or_else(|| DeployError::UnknownVersion { job: request.job.clone(), version: target.clone() })?;
        self.run_pipeline(&request.job, spec, version, &[], DeploymentKind::Rollback, &request.context).await
    }

    /// Deploys each job's baseline version, unless it's already running.
    pub async fn baseline(
        &self,
        jobs: Option<Vec<String>>,
        context: RequestContext,
    ) -> Vec<Result<Deployment, String>> {
        let catalogue = match self.catalogue() {
            Ok(c) => c,
            Err(err) => return vec![Err(err.to_string())],
        };
        let names = jobs.unwrap_or_else(|| catalogue.jobs.keys().cloned().collect());
        let mut results = Vec::new();
        for job in names {
            let Some(spec) = catalogue.job(&job) else {
                results.push(Err(format!("no job named {job}")));
                continue;
            };
            let current = self.current(&job).await.ok().flatten();
            let running = matches!(self.flink.active_job(&job).await, Ok(Some(run)) if run.state == JobState::Running);
            if running && current.as_ref().is_some_and(|(record, _)| record.current_version == spec.baseline) {
                continue;
            }
            let request = DeployRequest { job, version: spec.baseline.clone(), args: vec![], context: context.clone() };
            results.push(self.deploy(request).await.map_err(|e| e.to_string()));
        }
        results
    }

    /// The job's registry record and latest deployment.
    async fn current(&self, job: &str) -> anyhow::Result<Option<(JobRecord, Option<Deployment>)>> {
        if let Some((record, deployment)) = self.registry.lock().unwrap().get(job).cloned() {
            return Ok(Some((record, Some(deployment))));
        }
        let Some(record) = self.db.job(job).await? else { return Ok(None) };
        let deployment = self
            .db
            .deployments_before(job, Utc::now(), 20)
            .await?
            .into_iter()
            .find(|d| d.outcome == DeploymentOutcome::Succeeded && d.to_version == record.current_version);
        Ok(Some((record, deployment)))
    }

    async fn previous_version(&self, job: &str) -> Result<String, DeployError> {
        let current = self.current(job).await?;
        current
            .and_then(|(_, deployment)| deployment)
            .and_then(|d| d.from_version)
            .ok_or_else(|| DeployError::NoPreviousVersion(job.to_string()))
    }

    async fn run_pipeline(
        &self,
        job: &str,
        spec: &JobSpec,
        version: &Version,
        extra_args: &[String],
        kind: DeploymentKind,
        context: &RequestContext,
    ) -> Result<Deployment, DeployError> {
        let current = self.current(job).await?;
        let from_version = current.as_ref().map(|(record, _)| record.current_version.clone());
        let args: Vec<String> = match spec.kind {
            JobKind::Java => std::iter::once(format!("--job={job}"))
                .chain(version.args.iter().cloned())
                .chain(extra_args.iter().cloned())
                .collect(),
            JobKind::Sql => vec![],
        };
        info!(job, version = version.version, ?kind, from = ?from_version, "deploying");

        let result = async {
            let restore_from = match self.flink.active_job(job).await? {
                Some(run) => self.stop(job, &run).await?,
                None => None,
            };
            let jid = self.submit(job, spec, &args, restore_from.as_deref()).await?;
            self.wait_started(&jid).await?;
            anyhow::Ok((jid, restore_from))
        }
        .await;

        let now = Utc::now();
        let (outcome, flink_job_id, restored_from, failure_reason) = match result {
            Ok((jid, restored)) => (DeploymentOutcome::Succeeded, Some(jid), restored, None),
            Err(err) => {
                warn!(job, version = version.version, error = %format!("{err:#}"), "deploy failed");
                (DeploymentOutcome::Failed, None, None, Some(format!("{err:#}")))
            }
        };
        let deployment = Deployment {
            event_id: Uuid::new_v4(),
            ts: now,
            job_name: job.to_string(),
            tenant: spec.tenant.clone(),
            kind,
            from_version,
            to_version: version.version.clone(),
            commit_sha: version.commit_sha.clone(),
            author: version.author.clone(),
            summary: version.summary.clone(),
            args,
            triggered_by: context.triggered_by.clone(),
            outcome,
            failure_reason,
            flink_job_id: flink_job_id.clone(),
            restored_from,
        };
        self.publisher.publish(topics::DEPLOYMENTS, job, &deployment).await?;
        if outcome == DeploymentOutcome::Succeeded {
            let record = JobRecord {
                job_name: job.to_string(),
                tenant: spec.tenant.clone(),
                owner: spec.owner.clone(),
                tier: spec.tier,
                kind: spec.kind,
                current_version: version.version.clone(),
                flink_job_id,
                max_parallelism: spec.max_parallelism,
                deployed_at: now,
                first_deployed_at: current.map(|(record, _)| record.first_deployed_at).unwrap_or(now),
            };
            self.publisher.publish(topics::JOBS, job, &record).await?;
            self.registry.lock().unwrap().insert(job.to_string(), (record, deployment.clone()));
        }
        if let Some(action_id) = context.action_id {
            self.record_action(action_id, &deployment, context).await?;
        }
        info!(job, version = version.version, ?outcome, restored_from = ?deployment.restored_from, "deploy finished");
        Ok(deployment)
    }

    /// Stops a running version and returns where the next run restores from.
    async fn stop(&self, job: &str, run: &JobOverview) -> anyhow::Result<Option<String>> {
        if run.state == JobState::Running {
            match self.stop_with_savepoint(&run.jid).await {
                Ok(location) => {
                    self.wait_terminal(&run.jid).await?;
                    return Ok(Some(location));
                }
                Err(err) => warn!(job, jid = run.jid, %err, "stop-with-savepoint failed; cancelling"),
            }
        }
        self.flink.cancel(&run.jid).await?;
        self.wait_terminal(&run.jid).await?;
        let restore = self.db.latest_restore_point(job).await?.map(|point| point.path);
        info!(job, restore_from = ?restore, "restoring from the newest savepoint or retained checkpoint");
        Ok(restore)
    }

    async fn stop_with_savepoint(&self, jid: &str) -> anyhow::Result<String> {
        let trigger = self.flink.stop_with_savepoint(jid).await?;
        let deadline = tokio::time::Instant::now() + self.savepoint_timeout;
        while tokio::time::Instant::now() < deadline {
            match self.flink.savepoint_status(jid, &trigger).await? {
                SavepointStatus::Completed { location } => return Ok(location),
                SavepointStatus::Failed { cause } => anyhow::bail!("savepoint failed: {cause}"),
                SavepointStatus::InProgress => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        anyhow::bail!("savepoint timed out after {:?}", self.savepoint_timeout)
    }

    async fn wait_terminal(&self, jid: &str) -> anyhow::Result<()> {
        for _ in 0..120 {
            match self.flink.job_state(jid).await? {
                Some(state) if !state.is_terminal() => tokio::time::sleep(Duration::from_millis(500)).await,
                _ => return Ok(()),
            }
        }
        anyhow::bail!("job {jid} did not stop within 60 s")
    }

    async fn submit(
        &self,
        job: &str,
        spec: &JobSpec,
        args: &[String],
        restore_from: Option<&str>,
    ) -> anyhow::Result<String> {
        match spec.kind {
            JobKind::Java => {
                let jar_id = self.flink.upload_jar(&self.jar).await?;
                let result = self.flink.run_jar(&jar_id, ENTRY_CLASS, args, restore_from).await;
                if let Err(err) = self.flink.delete_jar(&jar_id).await {
                    warn!(%err, "deleting the uploaded jar");
                }
                Ok(result?)
            }
            JobKind::Sql => {
                let path = self.sql_dir.join(format!("{job}.sql"));
                let script = tokio::fs::read_to_string(&path).await?;
                let mut statements = Vec::new();
                if let Some(path) = restore_from {
                    statements.push(format!("SET 'execution.savepoint.path' = '{path}'"));
                    statements.push("SET 'execution.savepoint.ignore-unclaimed-state' = 'true'".to_string());
                }
                statements.extend(sql_gateway::statements(&script));
                self.gateway
                    .run(job, &statements)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("the SQL Gateway returned no job ID"))
            }
        }
    }

    /// Waits until Flink has started the job. A version that crashes once it
    /// reads data (F1) still counts as deployed: the deploy succeeded, the
    /// code is broken.
    async fn wait_started(&self, jid: &str) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + self.start_timeout;
        while tokio::time::Instant::now() < deadline {
            match self.flink.job_state(jid).await? {
                Some(JobState::Running | JobState::Restarting | JobState::Failing) => return Ok(()),
                Some(state) if state.is_terminal() => anyhow::bail!("job {jid} ended as {}", state.as_str()),
                _ => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        anyhow::bail!("job {jid} did not start within {:?}", self.start_timeout)
    }

    async fn record_action(&self, action_id: Uuid, d: &Deployment, context: &RequestContext) -> anyhow::Result<()> {
        let action = match d.kind {
            DeploymentKind::Rollback => ActionKind::RollbackToPreviousVersion,
            DeploymentKind::Deploy => ActionKind::Redeploy,
        };
        let event = ActionEvent {
            event_id: Uuid::new_v4(),
            ts: d.ts,
            action_id,
            job_name: d.job_name.clone(),
            incident_id: context.incident_id,
            signal_id: context.signal_id,
            actor: "cicd-sim".into(),
            action,
            phase: match d.outcome {
                DeploymentOutcome::Succeeded => ActionPhase::Executed,
                DeploymentOutcome::Failed => ActionPhase::Failed,
            },
            params: serde_json::json!({
                "from_version": d.from_version, "to_version": d.to_version, "args": d.args,
                "restored_from": d.restored_from, "deployment_event_id": d.event_id,
            }),
            rationale: d.failure_reason.clone(),
            ticket: None,
        };
        self.publisher.publish(topics::ACTIONS, &d.job_name, &event).await
    }
}
