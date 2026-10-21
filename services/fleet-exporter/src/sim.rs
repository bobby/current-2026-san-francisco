//! Per-job simulation state: turns `fleet.telemetry` steps into the
//! `platform.*` events a real job would produce, and keeps the values its
//! Prometheus metrics report.
//!
//! The rules mirror what Flink and the collector do for real jobs:
//! - each deploy starts a new run (new Flink job ID; restarts and checkpoint
//!   counters start again), restored from a savepoint of the old run;
//! - a failure is a `RESTARTING` status event plus the WARN log Flink writes
//!   for it; checkpoints aren't taken while the job is down;
//! - a checkpoint over the 30 s timeout fails as expired;
//! - while the job is down its input backs up, and it catches up afterwards.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use platform_model::{
    CheckpointEvent, CheckpointStatus, Deployment, DeploymentKind, DeploymentOutcome, Failure, JobRecord, JobState,
    JobStatusEvent, LogEvent, LogLevel, event_id,
    fleet::{FleetEvent, FleetEventKind, FleetFailure},
};

/// Flink's checkpoint timeout on the platform (flink/conf/config.yaml).
const CHECKPOINT_TIMEOUT_MS: f64 = 30_000.0;
/// How much faster than its input rate a job drains a backlog.
const CATCH_UP: f64 = 0.5;

/// An event to publish.
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Job(JobRecord),
    Deployment(Deployment),
    Status(JobStatusEvent),
    Checkpoint(CheckpointEvent),
    Log(LogEvent),
}

/// What the exporter knows about one simulated job.
#[derive(Debug, Clone)]
pub struct SimJob {
    pub info: FleetEvent,
    pub version: String,
    /// Runs so far; each deploy starts a new one.
    pub run: u32,
    pub flink_job_id: String,
    pub deployed_at: DateTime<Utc>,
    pub running: bool,
    /// When the current state (running or down) began.
    pub since: DateTime<Utc>,
    pub restarts: u64,
    pub checkpoint_id: u64,
    pub completed_checkpoints: u64,
    pub failed_checkpoints: u64,
    pub last_checkpoint: Option<(f64, f64, f64, DateTime<Utc>)>,
    pub state_size_bytes: f64,
    /// The latest task gauges from `st-fleet`.
    pub records_in_per_sec: f64,
    pub busy_ms_per_sec: f64,
    pub backpressured_ms_per_sec: f64,
    pub pending_records: f64,
    pub heap_used_ratio: f64,
    /// Records that queued up while the job was down.
    pub backlog: f64,
    pub last_update: DateTime<Utc>,
    /// Offsets consumed per input partition.
    pub offsets: Vec<f64>,
}

/// Lower-case hex of a deterministic UUID, for Flink-style 32-character IDs.
pub fn flink_id(parts: &[&str]) -> String {
    event_id(parts).simple().to_string()
}

impl SimJob {
    fn new(info: FleetEvent, now: DateTime<Utc>) -> Self {
        let parallelism = info.parallelism.max(1) as usize;
        let mut job = Self {
            version: info.version.clone(),
            run: 0,
            flink_job_id: String::new(),
            deployed_at: now,
            running: true,
            since: now,
            restarts: 0,
            checkpoint_id: 0,
            completed_checkpoints: 0,
            failed_checkpoints: 0,
            last_checkpoint: None,
            state_size_bytes: 0.0,
            records_in_per_sec: info.base_rate,
            busy_ms_per_sec: 0.0,
            backpressured_ms_per_sec: 0.0,
            pending_records: 0.0,
            heap_used_ratio: 0.3,
            backlog: 0.0,
            last_update: now,
            offsets: vec![0.0; parallelism * 3],
            info,
        };
        job.flink_job_id = job.run_id();
        job
    }

    fn run_id(&self) -> String {
        flink_id(&["fleet", &self.info.job_name, &self.run.to_string()])
    }

    /// Input records per second the job actually consumes right now.
    pub fn consumed_per_sec(&self) -> f64 {
        match (self.running, self.backlog > 0.0) {
            (false, _) => 0.0,
            (true, true) => self.records_in_per_sec * (1.0 + CATCH_UP),
            (true, false) => self.records_in_per_sec,
        }
    }

    /// Advances the backlog and offsets to `now`.
    fn advance(&mut self, now: DateTime<Utc>) {
        let dt = (now - self.last_update).num_milliseconds().max(0) as f64 / 1000.0;
        if self.running {
            let consumed = self.consumed_per_sec() * dt;
            self.backlog = (self.backlog - self.records_in_per_sec * CATCH_UP * dt).max(0.0);
            let per_partition = consumed / self.offsets.len() as f64;
            self.offsets.iter_mut().for_each(|o| *o += per_partition);
        } else {
            self.backlog += self.info.base_rate * dt;
        }
        self.last_update = now;
    }

    fn record(&self) -> JobRecord {
        JobRecord {
            job_name: self.info.job_name.clone(),
            tenant: self.info.tenant.clone(),
            owner: self.info.owner.clone(),
            tier: self.info.tier,
            kind: self.info.kind,
            current_version: self.version.clone(),
            flink_job_id: Some(self.flink_job_id.clone()),
            max_parallelism: self.info.max_parallelism,
            deployed_at: self.deployed_at,
            first_deployed_at: self.info.first_deployed_at,
        }
    }

    fn status(
        &self,
        now: DateTime<Utc>,
        status: JobState,
        previous: Option<JobState>,
        failure: Option<Failure>,
    ) -> JobStatusEvent {
        JobStatusEvent {
            event_id: event_id(&[&self.flink_job_id, status.as_str(), &now.timestamp_millis().to_string()]),
            ts: now,
            job_name: self.info.job_name.clone(),
            flink_job_id: self.flink_job_id.clone(),
            status,
            previous_status: previous,
            restarts: self.restarts,
            failure,
        }
    }

    fn log(
        &self,
        now: DateTime<Utc>,
        level: LogLevel,
        logger: &str,
        message: String,
        failure: Option<&Failure>,
    ) -> LogEvent {
        let host = format!("10.44.{}.{}", self.cluster_number(), self.host_number());
        LogEvent {
            event_id: event_id(&["fleet-log", &self.flink_job_id, &now.timestamp_millis().to_string(), &message]),
            ts: now,
            job_name: self.info.job_name.clone(),
            flink_job_id: Some(self.flink_job_id.clone()),
            level,
            component: "taskmanager".into(),
            host,
            logger: logger.into(),
            message,
            exception_class: failure.map(|f| f.exception_class.clone()),
            exception_message: failure.and_then(|f| f.message.clone()),
            root_cause_class: failure.and_then(|f| f.root_cause_class.clone()),
            root_cause_message: failure.and_then(|f| f.root_cause_message.clone()),
            stack_head: failure.and_then(|f| f.stack_head.clone()),
        }
    }

    pub fn cluster_number(&self) -> u32 {
        self.info.cluster.rsplit('-').next().and_then(|n| n.parse().ok()).unwrap_or(0)
    }

    /// The last octet of the TaskManager running the job.
    pub fn host_number(&self) -> u32 {
        let id = flink_id(&["fleet-tm", &self.info.job_name]);
        u32::from_str_radix(&id[..4], 16).unwrap_or(0) % 200 + 20
    }

    /// The source task's display name, as Flink builds it from the operator chain.
    pub fn task_name(&self) -> String {
        format!("Source: {}[1] -> Calc[2] -> IcebergStreamWriter", self.info.input_topic.replace(['.', '-'], "_"))
    }

    pub fn source_name(&self) -> String {
        format!("Source: {}[1]", self.info.input_topic.replace(['.', '-'], "_"))
    }
}

fn failure_from(f: &FleetFailure, task: &str) -> Failure {
    let mut head = format!("{}: {}", f.exception_class, f.message.as_deref().unwrap_or_default());
    head.push_str("\n\tat org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:612)");
    if let Some(root) = &f.root_cause_class {
        head.push_str(&format!("\nCaused by: {root}"));
        if let Some(message) = &f.root_cause_message {
            head.push_str(&format!(": {message}"));
        }
    }
    Failure {
        exception_class: f.exception_class.clone(),
        message: f.message.clone(),
        root_cause_class: f.root_cause_class.clone().or_else(|| Some(f.exception_class.clone())),
        root_cause_message: f.root_cause_message.clone(),
        stack_head: Some(head),
        task: Some(format!("{} (1/1)", task)),
    }
}

/// Bumps the minor version: 2.14.3 → 2.15.0.
fn next_version(version: &str) -> String {
    let parts: Vec<u64> = version.split('.').filter_map(|p| p.parse().ok()).collect();
    match parts.as_slice() {
        [major, minor, ..] => format!("{major}.{}.0", minor + 1),
        _ => format!("{version}.1"),
    }
}

#[derive(Default)]
pub struct Fleet {
    pub jobs: HashMap<String, SimJob>,
}

impl Fleet {
    /// Applies one `fleet.telemetry` step and returns the events to publish.
    pub fn apply(&mut self, event: FleetEvent, now: DateTime<Utc>) -> Vec<Output> {
        let mut out = Vec::new();
        let name = event.job_name.clone();
        let job = match self.jobs.get_mut(&name) {
            Some(job) => job,
            None => {
                // First sight: register the job and its running run.
                let job = SimJob::new(event.clone(), now);
                out.push(Output::Job(job.record()));
                out.push(Output::Status(job.status(now, JobState::Running, None, None)));
                self.jobs.entry(name.clone()).or_insert(job)
            }
        };
        job.advance(now);
        job.info = FleetEvent { event: FleetEventKind::Recovered {}, ..event.clone() };

        match event.event {
            FleetEventKind::Metrics {
                records_in_per_sec,
                busy_ms_per_sec,
                backpressured_ms_per_sec,
                pending_records,
                heap_used_ratio,
            } => {
                job.records_in_per_sec = records_in_per_sec;
                job.busy_ms_per_sec = busy_ms_per_sec;
                job.backpressured_ms_per_sec = backpressured_ms_per_sec;
                job.pending_records = pending_records;
                job.heap_used_ratio = heap_used_ratio;
            }
            FleetEventKind::Checkpoint { failed, duration_ms, size_bytes, state_size_bytes, failure_reason } => {
                let expired = duration_ms > CHECKPOINT_TIMEOUT_MS;
                let failed = failed || expired;
                // Flink doesn't trigger checkpoints while the job is down,
                // but one in flight when it fails is reported as failed.
                if job.running || failed {
                    job.checkpoint_id += 1;
                    let duration = duration_ms.min(CHECKPOINT_TIMEOUT_MS);
                    let status = if failed { CheckpointStatus::Failed } else { CheckpointStatus::Completed };
                    let id = job.checkpoint_id.to_string();
                    if failed {
                        job.failed_checkpoints += 1;
                    } else {
                        job.completed_checkpoints += 1;
                        job.state_size_bytes = state_size_bytes;
                        job.last_checkpoint = Some((duration, size_bytes, state_size_bytes, now));
                    }
                    let trigger_ts = now - chrono::Duration::milliseconds(duration as i64);
                    out.push(Output::Checkpoint(CheckpointEvent {
                        event_id: event_id(&[&job.flink_job_id, "checkpoint", &id]),
                        ts: now,
                        job_name: name.clone(),
                        flink_job_id: job.flink_job_id.clone(),
                        checkpoint_id: job.checkpoint_id,
                        status,
                        is_savepoint: false,
                        trigger_ts,
                        duration_ms: duration as u64,
                        checkpointed_size_bytes: if failed { 0 } else { size_bytes as u64 },
                        state_size_bytes: state_size_bytes as u64,
                        failure_reason: if failed {
                            failure_reason
                                .or_else(|| expired.then(|| "Checkpoint expired before completing.".to_string()))
                        } else {
                            None
                        },
                        external_path: (!failed)
                            .then(|| format!("s3://{}/checkpoints/{}/chk-{id}", job.info.cluster, job.flink_job_id)),
                    }));
                }
            }
            FleetEventKind::Failure { failure } => {
                let previous = if job.running { JobState::Running } else { JobState::Restarting };
                job.restarts += 1;
                if job.running {
                    job.running = false;
                    job.since = now;
                }
                let failure = failure_from(&failure, &job.task_name());
                let task = job.task_name();
                out.push(Output::Status(job.status(now, JobState::Restarting, Some(previous), Some(failure.clone()))));
                out.push(Output::Log(job.log(
                    now,
                    LogLevel::Warn,
                    "org.apache.flink.runtime.taskmanager.Task",
                    format!("{task} (1/1)#{} switched from RUNNING to FAILED with failure cause:", job.restarts - 1),
                    Some(&failure),
                )));
            }
            FleetEventKind::Recovered {} => {
                if !job.running {
                    job.running = true;
                    job.since = now;
                    out.push(Output::Status(job.status(now, JobState::Running, Some(JobState::Restarting), None)));
                }
            }
            FleetEventKind::Deploy { summary, commit_sha } => {
                let from = job.version.clone();
                let old_run = job.flink_job_id.clone();
                let savepoint = format!(
                    "s3://{}/savepoints/savepoint-{}-{}",
                    job.info.cluster,
                    &old_run[..6],
                    &commit_sha[..12.min(commit_sha.len())]
                );
                out.push(Output::Status(job.status(now, JobState::Finished, Some(JobState::Running), None)));
                job.run += 1;
                job.flink_job_id = job.run_id();
                job.version = next_version(&from);
                job.deployed_at = now;
                job.running = true;
                job.since = now;
                job.restarts = 0;
                job.checkpoint_id = 0;
                job.completed_checkpoints = 0;
                job.failed_checkpoints = 0;
                job.last_checkpoint = None;
                out.push(Output::Deployment(Deployment {
                    event_id: event_id(&["fleet-deploy", &job.flink_job_id]),
                    ts: now,
                    job_name: name.clone(),
                    tenant: job.info.tenant.clone(),
                    kind: DeploymentKind::Deploy,
                    from_version: Some(from),
                    to_version: job.version.clone(),
                    commit_sha: commit_sha.chars().take(7).collect(),
                    author: job.info.author.clone(),
                    summary,
                    args: vec![],
                    triggered_by: "ci".into(),
                    outcome: DeploymentOutcome::Succeeded,
                    failure_reason: None,
                    flink_job_id: Some(job.flink_job_id.clone()),
                    restored_from: Some(savepoint),
                }));
                out.push(Output::Job(job.record()));
                out.push(Output::Status(job.status(now, JobState::Running, None, None)));
            }
            FleetEventKind::Log { level, logger, message } => {
                out.push(Output::Log(job.log(now, level, &logger, message, None)));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(kind: FleetEventKind) -> FleetEvent {
        FleetEvent {
            job_name: "card-auth-txn-scorer".into(),
            tenant: "card-auth".into(),
            owner: "@card-auth".into(),
            tier: platform_model::Tier::Tier1,
            kind: platform_model::JobKind::Java,
            max_parallelism: 4,
            parallelism: 2,
            cluster: "flink-03".into(),
            input_topic: "card-auth.txns".into(),
            sink_table: "card_auth.txns".into(),
            first_deployed_at: "2025-01-01T00:00:00Z".parse().unwrap(),
            version: "2.14.3".into(),
            author: "nina.patel".into(),
            base_rate: 100.0,
            heap_max_bytes: 2e9,
            event: kind,
        }
    }

    fn at(s: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + s, 0).unwrap()
    }

    fn checkpoint(duration_ms: f64) -> FleetEventKind {
        FleetEventKind::Checkpoint {
            failed: false,
            duration_ms,
            size_bytes: 1e6,
            state_size_bytes: 8e6,
            failure_reason: None,
        }
    }

    fn failure() -> FleetEventKind {
        FleetEventKind::Failure {
            failure: FleetFailure {
                exception_class: "java.lang.IllegalStateException".into(),
                message: Some("Unknown currency code: XAU".into()),
                root_cause_class: None,
                root_cause_message: None,
            },
        }
    }

    fn kinds(out: &[Output]) -> Vec<String> {
        out.iter()
            .map(|o| match o {
                Output::Job(_) => "job".into(),
                Output::Deployment(_) => "deployment".into(),
                Output::Status(s) => format!("status:{}", s.status.as_str()),
                Output::Checkpoint(c) => format!("checkpoint:{:?}", c.status),
                Output::Log(_) => "log".into(),
            })
            .collect()
    }

    #[test]
    fn first_sight_registers_the_job() {
        let mut fleet = Fleet::default();
        let out = fleet.apply(event(checkpoint(500.0)), at(0));
        assert_eq!(kinds(&out), ["job", "status:RUNNING", "checkpoint:Completed"]);
        let Output::Checkpoint(c) = &out[2] else { panic!() };
        assert_eq!(c.checkpoint_id, 1);
        assert!(c.external_path.as_ref().unwrap().starts_with("s3://flink-03/checkpoints/"));
    }

    #[test]
    fn failure_and_recovery() {
        let mut fleet = Fleet::default();
        fleet.apply(event(checkpoint(500.0)), at(0));
        let out = fleet.apply(event(failure()), at(10));
        assert_eq!(kinds(&out), ["status:RESTARTING", "log"]);
        let Output::Status(s) = &out[0] else { panic!() };
        assert_eq!(s.restarts, 1);
        assert_eq!(s.failure.as_ref().unwrap().root_cause_class.as_deref(), Some("java.lang.IllegalStateException"));
        // No checkpoints while down; the input backs up.
        assert!(fleet.apply(event(checkpoint(500.0)), at(20)).is_empty());
        assert_eq!(fleet.jobs["card-auth-txn-scorer"].backlog, 1000.0);
        let out = fleet.apply(event(FleetEventKind::Recovered {}), at(30));
        assert_eq!(kinds(&out), ["status:RUNNING"]);
        let job = &fleet.jobs["card-auth-txn-scorer"];
        assert_eq!(job.backlog, 2000.0);
        assert_eq!(job.consumed_per_sec(), 150.0);
    }

    #[test]
    fn slow_checkpoints_expire() {
        let mut fleet = Fleet::default();
        let out = fleet.apply(event(checkpoint(45_000.0)), at(0));
        let Output::Checkpoint(c) = &out[2] else { panic!() };
        assert_eq!((c.status, c.duration_ms), (CheckpointStatus::Failed, 30_000));
        assert_eq!(c.failure_reason.as_deref(), Some("Checkpoint expired before completing."));
        assert_eq!(fleet.jobs["card-auth-txn-scorer"].failed_checkpoints, 1);
    }

    #[test]
    fn deploy_starts_a_new_run() {
        let mut fleet = Fleet::default();
        fleet.apply(event(failure()), at(0));
        let old = fleet.jobs["card-auth-txn-scorer"].flink_job_id.clone();
        let out = fleet.apply(
            event(FleetEventKind::Deploy { summary: "Upgrade Jackson".into(), commit_sha: "9f2c1ab0".repeat(8) }),
            at(10),
        );
        assert_eq!(kinds(&out), ["status:FINISHED", "deployment", "job", "status:RUNNING"]);
        let Output::Deployment(d) = &out[1] else { panic!() };
        assert_eq!((d.from_version.as_deref(), d.to_version.as_str()), (Some("2.14.3"), "2.15.0"));
        assert_eq!(d.commit_sha, "9f2c1ab");
        let job = &fleet.jobs["card-auth-txn-scorer"];
        assert_ne!(job.flink_job_id, old);
        assert_eq!((job.restarts, job.flink_job_id.len()), (0, 32));
    }
}
