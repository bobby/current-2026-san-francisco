//! `fleet.telemetry`: the simulated fleet's raw lifecycle stream
//! (docs/design.md §4.4, §5.2).
//!
//! ShadowTraffic's `st-fleet` runs one state machine per simulated job and
//! writes each step here: a metrics sample, a checkpoint, a failure, a
//! recovery, a deploy or a log line. A ShadowTraffic generator writes to one
//! topic, so the fleet-exporter fans this stream out into the same
//! `platform.*` events real jobs produce, keeping each job's counters, run
//! IDs and versions consistent, and serves its Prometheus metrics. Agents
//! never read this topic.
//!
//! Every event carries the job's roster entry (shadowtraffic/fleet/roster.py),
//! so the exporter needs no other state to describe the job.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{JobKind, LogLevel, Tier, Timestamp};

pub const TOPIC: &str = "fleet.telemetry";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FleetEvent {
    pub job_name: String,
    pub tenant: String,
    pub owner: String,
    pub tier: Tier,
    pub kind: JobKind,
    pub max_parallelism: u32,
    pub parallelism: u32,
    /// The fleet cluster running the job; its JobManager's host name.
    pub cluster: String,
    pub input_topic: String,
    pub sink_table: String,
    pub first_deployed_at: Timestamp,
    /// The version running when the simulation starts.
    pub version: String,
    pub author: String,
    /// Normal input rate, records per second.
    pub base_rate: f64,
    pub heap_max_bytes: f64,
    #[serde(flatten)]
    pub event: FleetEventKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum FleetEventKind {
    /// Task gauges, sampled every tick while the job runs.
    Metrics {
        records_in_per_sec: f64,
        busy_ms_per_sec: f64,
        backpressured_ms_per_sec: f64,
        /// Consumer lag from the input, not counting backlog the exporter
        /// adds while the job is down.
        pending_records: f64,
        heap_used_ratio: f64,
    },
    /// A checkpoint finished. One that takes longer than the 30 s checkpoint
    /// timeout is reported as failed (expired).
    Checkpoint {
        #[serde(default)]
        failed: bool,
        duration_ms: f64,
        size_bytes: f64,
        state_size_bytes: f64,
        failure_reason: Option<String>,
    },
    /// The job failed and is restarting.
    Failure {
        failure: FleetFailure,
    },
    /// The job is running again after a failure.
    Recovered {},
    /// A new version was deployed.
    Deploy {
        summary: String,
        commit_sha: String,
    },
    Log {
        level: LogLevel,
        logger: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FleetFailure {
    pub exception_class: String,
    pub message: Option<String>,
    pub root_cause_class: Option<String>,
    pub root_cause_message: Option<String>,
}
