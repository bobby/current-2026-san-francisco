//! Polls the Flink REST API and publishes `platform.job-status` and
//! `platform.checkpoints` events.
//!
//! A restart loop can fail and recover between two polls, so failures come
//! from each job's exception history rather than from state changes alone.
//! Event IDs are derived from Flink's own identifiers and timestamps, so
//! after a collector restart the re-read history is stored once in the ops DB.

use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use chrono::{DateTime, Utc};
use platform_clients::{
    flink::{CheckpointEntry, CheckpointState, ExceptionEntry, FlinkClient, JobOverview},
    stack_trace,
};
use platform_model::{
    CheckpointEvent, CheckpointStatus, Failure, JobState, JobStatusEvent, Timestamp, event_id, topics,
};
use tracing::{debug, warn};

use crate::{jobs::JobNames, publisher::Publisher};

/// Exception history entries read per poll.
const MAX_EXCEPTIONS: usize = 20;
/// Checkpoint IDs remembered per run, to publish each checkpoint once.
const SEEN_CHECKPOINTS: usize = 200;

pub async fn run(flink: FlinkClient, publisher: Publisher, names: JobNames, interval: Duration) -> anyhow::Result<()> {
    let mut tracker = Tracker::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(err) = poll(&flink, &publisher, &names, &mut tracker).await {
            warn!(%err, "polling Flink");
        }
    }
}

async fn poll(
    flink: &FlinkClient,
    publisher: &Publisher,
    names: &JobNames,
    tracker: &mut Tracker,
) -> anyhow::Result<()> {
    let jobs = flink.jobs().await?;
    tracker.forget_missing(&jobs);
    for job in &jobs {
        names.insert(&job.jid, &job.name);
        if tracker.is_done(&job.jid) {
            continue;
        }
        let exceptions = flink.exceptions(&job.jid, MAX_EXCEPTIONS).await?;
        let restarts = flink.job_metrics(&job.jid, &["numRestarts"]).await?.get("numRestarts").map(|r| *r as u64);
        let checkpoints = flink.checkpoints(&job.jid).await?;
        let events = tracker.observe(job, &exceptions, restarts, &checkpoints);
        for event in events {
            debug!(?event, "publishing");
            match &event {
                Event::Status(e) => publisher.publish(topics::JOB_STATUS, &e.job_name, e).await?,
                Event::Checkpoint(e) => publisher.publish(topics::CHECKPOINTS, &e.job_name, e).await?,
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Status(JobStatusEvent),
    Checkpoint(CheckpointEvent),
}

/// What the collector knows about each Flink run.
#[derive(Default)]
pub struct Tracker {
    runs: HashMap<String, Run>,
}

#[derive(Default)]
struct Run {
    state: Option<JobState>,
    /// Timestamp (epoch ms) of the newest failure published.
    last_failure: Option<i64>,
    restarts: u64,
    checkpoints: BTreeSet<u64>,
    /// The run has terminated and its final events are published.
    done: bool,
}

fn millis(ms: i64) -> Timestamp {
    DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default()
}

impl Tracker {
    fn is_done(&self, jid: &str) -> bool {
        self.runs.get(jid).is_some_and(|run| run.done)
    }

    /// Drops runs Flink no longer reports.
    fn forget_missing(&mut self, jobs: &[JobOverview]) {
        self.runs.retain(|jid, _| jobs.iter().any(|job| &job.jid == jid));
    }

    /// Compares one poll of a job with what was seen before and returns the
    /// events to publish, oldest first.
    pub fn observe(
        &mut self,
        job: &JobOverview,
        exceptions: &[ExceptionEntry],
        restarts: Option<u64>,
        checkpoints: &[CheckpointEntry],
    ) -> Vec<Event> {
        let run = self.runs.entry(job.jid.clone()).or_default();
        let mut events = Vec::new();

        // Failures, oldest first. Each one restarted the job.
        let mut new_failures: Vec<&ExceptionEntry> =
            exceptions.iter().filter(|e| run.last_failure.is_none_or(|last| e.timestamp > last)).collect();
        new_failures.sort_by_key(|e| e.timestamp);
        // Flink's numRestarts metric can lag a poll behind, and the exception
        // history only covers recent failures, so take whichever is higher.
        let restarts_now = (run.restarts + new_failures.len() as u64).max(restarts.unwrap_or(0));
        let count = new_failures.len();
        for (i, entry) in new_failures.into_iter().enumerate() {
            let newest = i + 1 == count;
            let status = if newest && job.state == JobState::Failed { JobState::Failed } else { JobState::Restarting };
            let summary = stack_trace::summarize(&entry.stacktrace);
            let top = stack_trace::summarize(entry.stacktrace.lines().next().unwrap_or_default());
            events.push(Event::Status(JobStatusEvent {
                event_id: event_id(&[&job.jid, "failure", &entry.timestamp.to_string()]),
                ts: millis(entry.timestamp),
                job_name: job.name.clone(),
                flink_job_id: job.jid.clone(),
                status,
                previous_status: run.state,
                restarts: restarts_now.saturating_sub((count - 1 - i) as u64),
                failure: Some(Failure {
                    exception_class: entry.exception_name.clone(),
                    message: top.root_cause_message,
                    root_cause_class: summary.root_cause_class,
                    root_cause_message: summary.root_cause_message,
                    stack_head: summary.head,
                    task: entry.task_name.clone(),
                }),
            }));
            run.state = Some(status);
            run.last_failure = Some(entry.timestamp);
        }
        run.restarts = restarts_now;

        // State changes, e.g. RESTARTING → RUNNING, or RUNNING → CANCELED.
        if run.state != Some(job.state) {
            events.push(Event::Status(JobStatusEvent {
                event_id: event_id(&[&job.jid, job.state.as_str(), &job.last_modification.to_string()]),
                ts: millis(job.last_modification),
                job_name: job.name.clone(),
                flink_job_id: job.jid.clone(),
                status: job.state,
                previous_status: run.state,
                restarts: run.restarts,
                failure: None,
            }));
            run.state = Some(job.state);
        }

        // Checkpoints that finished since the last poll, oldest first.
        let mut finished: Vec<&CheckpointEntry> = checkpoints
            .iter()
            .filter(|c| c.status != CheckpointState::InProgress && !run.checkpoints.contains(&c.id))
            .collect();
        finished.sort_by_key(|c| c.id);
        for c in finished {
            let status = match c.status {
                CheckpointState::Failed => CheckpointStatus::Failed,
                _ => CheckpointStatus::Completed,
            };
            let ended = match status {
                CheckpointStatus::Failed => c.failure_timestamp.unwrap_or(c.latest_ack_timestamp),
                CheckpointStatus::Completed => c.latest_ack_timestamp,
            };
            events.push(Event::Checkpoint(CheckpointEvent {
                event_id: event_id(&[&job.jid, "checkpoint", &c.id.to_string()]),
                ts: millis(ended.max(c.trigger_timestamp)),
                job_name: job.name.clone(),
                flink_job_id: job.jid.clone(),
                checkpoint_id: c.id,
                status,
                is_savepoint: c.is_savepoint,
                trigger_ts: millis(c.trigger_timestamp),
                duration_ms: c.end_to_end_duration,
                checkpointed_size_bytes: c.checkpointed_size,
                state_size_bytes: c.state_size,
                failure_reason: c.failure_message.clone(),
            }));
            run.checkpoints.insert(c.id);
        }
        while run.checkpoints.len() > SEEN_CHECKPOINTS {
            run.checkpoints.pop_first();
        }

        run.done = job.state.is_terminal();
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(state: JobState, last_modification: i64) -> JobOverview {
        serde_json::from_value(serde_json::json!({
            "jid": "jid-1", "name": "payments-enrichment", "state": state,
            "start-time": 1000, "last-modification": last_modification,
        }))
        .unwrap()
    }

    fn npe(timestamp: i64) -> ExceptionEntry {
        ExceptionEntry {
            exception_name: "java.io.IOException".into(),
            stacktrace: "java.io.IOException: Failed to deserialize consumer record due to\n\tat a.b(C.java:1)\nCaused by: java.lang.NullPointerException\n\tat d.e(F.java:2)".into(),
            timestamp,
            task_name: None,
        }
    }

    fn checkpoint(id: u64, status: CheckpointState) -> CheckpointEntry {
        serde_json::from_value(serde_json::json!({
            "id": id, "status": match status {
                CheckpointState::InProgress => "IN_PROGRESS",
                CheckpointState::Completed => "COMPLETED",
                CheckpointState::Failed => "FAILED",
            },
            "is_savepoint": false, "trigger_timestamp": id * 10_000, "latest_ack_timestamp": id * 10_000 + 250,
            "end_to_end_duration": 250, "checkpointed_size": 3780, "state_size": 3780,
        }))
        .unwrap()
    }

    fn statuses(events: &[Event]) -> Vec<(JobState, Option<JobState>, u64, bool)> {
        events
            .iter()
            .filter_map(|e| match e {
                Event::Status(s) => Some((s.status, s.previous_status, s.restarts, s.failure.is_some())),
                Event::Checkpoint(_) => None,
            })
            .collect()
    }

    #[test]
    fn first_sight_of_a_running_job() {
        let mut tracker = Tracker::default();
        let events = tracker.observe(&job(JobState::Running, 5000), &[], Some(0), &[]);
        assert_eq!(statuses(&events), [(JobState::Running, None, 0, false)]);
        // Nothing new on the next poll.
        assert!(tracker.observe(&job(JobState::Running, 5000), &[], Some(0), &[]).is_empty());
    }

    #[test]
    fn restart_loop_between_polls() {
        let mut tracker = Tracker::default();
        tracker.observe(&job(JobState::Running, 5000), &[], Some(0), &[]);
        // Two failures and a recovery happened between polls.
        let events = tracker.observe(&job(JobState::Running, 9000), &[npe(8000), npe(6000)], Some(2), &[]);
        assert_eq!(
            statuses(&events),
            [
                (JobState::Restarting, Some(JobState::Running), 1, true),
                (JobState::Restarting, Some(JobState::Restarting), 2, true),
                (JobState::Running, Some(JobState::Restarting), 2, false),
            ]
        );
        let Event::Status(first) = &events[0] else { panic!() };
        let failure = first.failure.as_ref().unwrap();
        assert_eq!(failure.exception_class, "java.io.IOException");
        assert_eq!(failure.message.as_deref(), Some("Failed to deserialize consumer record due to"));
        assert_eq!(failure.root_cause_class.as_deref(), Some("java.lang.NullPointerException"));
        // The same history isn't published twice.
        assert!(tracker.observe(&job(JobState::Running, 9000), &[npe(8000), npe(6000)], Some(2), &[]).is_empty());
    }

    #[test]
    fn checkpoints_are_published_once_when_finished() {
        let mut tracker = Tracker::default();
        let polled = [checkpoint(2, CheckpointState::InProgress), checkpoint(1, CheckpointState::Completed)];
        let events = tracker.observe(&job(JobState::Running, 5000), &[], Some(0), &polled);
        let ids = |events: &[Event]| -> Vec<u64> {
            events
                .iter()
                .filter_map(|e| if let Event::Checkpoint(c) = e { Some(c.checkpoint_id) } else { None })
                .collect()
        };
        assert_eq!(ids(&events), [1]);
        let polled = [
            checkpoint(3, CheckpointState::Failed),
            checkpoint(2, CheckpointState::Completed),
            checkpoint(1, CheckpointState::Completed),
        ];
        let events = tracker.observe(&job(JobState::Running, 5000), &[], Some(0), &polled);
        assert_eq!(ids(&events), [2, 3]);
        let Event::Checkpoint(failed) = &events[1] else { panic!() };
        assert_eq!(failed.status, CheckpointStatus::Failed);
    }

    #[test]
    fn terminal_runs_are_done() {
        let mut tracker = Tracker::default();
        tracker.observe(&job(JobState::Running, 5000), &[], Some(0), &[]);
        let events = tracker.observe(&job(JobState::Canceled, 7000), &[], Some(0), &[]);
        assert_eq!(statuses(&events), [(JobState::Canceled, Some(JobState::Running), 0, false)]);
        assert!(tracker.is_done("jid-1"));
    }
}
