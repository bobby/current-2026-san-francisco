//! The tools. Each returns JSON (or a short error) as text; times are UTC,
//! with ages in minutes where they help.

use std::time::Duration;

use chrono::{DateTime, Utc};
use platform_clients::{
    chatops::{Channel, ChatopsClient, ProposalRequest, TicketProposal},
    cicd::CicdClient,
    flink::{FlinkClient, job_parallelism},
    ops_db::OpsDb,
    prometheus::PrometheusClient,
    tickets::TicketsClient,
};
use platform_model::{ActionKind, Disposition, LogLevel};
use rdkafka::{
    ClientConfig, Offset, TopicPartitionList,
    consumer::{BaseConsumer, Consumer},
    message::Message,
};
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{Implementation, ServerCapabilities, ServerConfig},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

/// The Mattermost thread the conversation is in.
#[derive(Debug, Clone)]
pub struct Thread {
    pub root_id: Option<String>,
    pub channel: Option<String>,
    pub signal_id: Option<uuid::Uuid>,
}

#[derive(Clone)]
pub struct Platform {
    db: OpsDb,
    prometheus: PrometheusClient,
    flink: FlinkClient,
    cicd: CicdClient,
    tickets: TicketsClient,
    chatops: ChatopsClient,
    kafka_bootstrap_servers: String,
    thread: Thread,
    tool_router: ToolRouter<Self>,
}

/// How long the evidence tools look back by default.
const DEFAULT_MINUTES: u32 = 30;

fn minutes_ago(t: DateTime<Utc>) -> f64 {
    ((Utc::now() - t).num_seconds() as f64 / 6.0).round() / 10.0
}

fn json_text(value: Value) -> String {
    serde_json::to_string_pretty(&value).unwrap_or_else(|e| format!("couldn't encode the result: {e}"))
}

fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

/// Job arguments without fault flags: the evidence an operator would have
/// (the flags are how the demo injects bugs).
fn visible_args(args: &[String]) -> Vec<&String> {
    args.iter().filter(|a| !a.starts_with("--fault")).collect()
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SignalParams {
    /// A signal ID. Defaults to the signal this thread is about.
    pub signal_id: Option<String>,
    /// Or the latest raised signal for this job.
    pub job: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct JobParams {
    /// The platform job name, e.g. `trade-events-ingest`.
    pub job: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DeploymentsParams {
    pub job: String,
    /// How many, newest first (default 10).
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PrometheusParams {
    /// A PromQL query. Flink metrics carry `job_name` in Flink's form, with
    /// `-` replaced by `_` (e.g. `trade_events_ingest`).
    pub promql: String,
    /// Evaluate over the last this many minutes (a range query) instead of
    /// now.
    pub range_minutes: Option<u32>,
    /// The range query's step in seconds (default: about 20 points).
    pub step_seconds: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct LogsParams {
    pub job: String,
    /// How far back, in minutes (default 30).
    pub minutes: Option<u32>,
    /// Minimum level: WARN (default) or ERROR.
    pub level: Option<String>,
    /// Only messages containing this text.
    pub contains: Option<String>,
    /// At most this many, newest first (default 30).
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CheckpointsParams {
    pub job: String,
    /// How far back, in minutes (default 30).
    pub minutes: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SampleParams {
    /// A Kafka topic, e.g. `trade-events`.
    pub topic: String,
    /// One partition; all partitions if omitted.
    pub partition: Option<i32>,
    /// Read from this offset (needs `partition`); the latest records if
    /// omitted.
    pub offset: Option<i64>,
    /// How many records per partition (default 10, at most 50).
    pub count: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TicketParams {
    /// A ticket number, e.g. `INC0010001`.
    pub number: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchTicketsParams {
    /// Tickets about this job.
    pub job: Option<String>,
    /// Only open ones (default true).
    pub active: Option<bool>,
    /// At most this many, newest first (default 10).
    pub limit: Option<u32>,
}

/// A remediation the platform can carry out once a person approves it.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Remediation {
    /// Redeploy a version (usually the running one) through the deploy
    /// pipeline, with extra job arguments. Allowed arguments:
    /// `--on-deser-error=dlq|fail` (Java jobs: divert records that can't be
    /// parsed to `<topic>.dlq` instead of failing) and
    /// `--state-backend=rocksdb|hashmap`.
    Redeploy { version: String, args: Vec<String> },
    /// Roll back to the version before the current one, or to `to_version`.
    RollbackToPreviousVersion { to_version: Option<String> },
    /// Rescale the running job to this parallelism (within the tenant's
    /// maximum).
    ScaleUp { parallelism: u32 },
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProposeParams {
    pub job: String,
    pub remediation: Remediation,
    /// Why, in two or three sentences citing the evidence. Shown on the
    /// approval card.
    pub rationale: String,
    /// A ticket to open once approved, e.g. for the upstream team that
    /// needs to fix their data.
    pub ticket: Option<ProposedTicket>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ProposedTicket {
    /// The team to assign it to, e.g. `trading-data` or an upstream team.
    pub assignment_group: String,
    pub short_description: String,
    /// What happened and what they need to do, with the evidence.
    pub description: String,
}

const ALLOWED_ARGS: [&str; 4] =
    ["--on-deser-error=dlq", "--on-deser-error=fail", "--state-backend=rocksdb", "--state-backend=hashmap"];

impl Platform {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        ops_database_url: &str,
        prometheus_url: &str,
        flink_url: &str,
        cicd_url: &str,
        tickets_url: &str,
        chatops_url: &str,
        kafka_bootstrap_servers: &str,
        thread: Thread,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            db: OpsDb::connect(ops_database_url).await?,
            prometheus: PrometheusClient::new(prometheus_url),
            flink: FlinkClient::new(flink_url),
            cicd: CicdClient::new(cicd_url),
            tickets: TicketsClient::new(tickets_url, "ops-assistant"),
            chatops: ChatopsClient::new(chatops_url),
            kafka_bootstrap_servers: kafka_bootstrap_servers.to_string(),
            thread,
            tool_router: Self::tool_router(),
        })
    }

    async fn signal(&self, p: SignalParams) -> anyhow::Result<Value> {
        let signal = match (p.signal_id.as_deref(), p.job.as_deref(), self.thread.signal_id) {
            (Some(id), _, _) => self.db.signal(id.parse()?).await?,
            (None, Some(job), _) => {
                let now = Utc::now();
                let signals = self.db.signals_between(Some(job), now - chrono::Duration::hours(6), now).await?;
                signals.into_iter().rev().find(|s| s.disposition == Disposition::Raise)
            }
            (None, None, Some(id)) => self.db.signal(id).await?,
            (None, None, None) => anyhow::bail!("this thread isn't about a signal; give a signal_id or a job"),
        };
        let Some(s) = signal else { anyhow::bail!("no such signal") };
        Ok(json!({
            "signal_id": s.signal_id,
            "at": s.ts,
            "minutes_ago": minutes_ago(s.ts),
            "job": s.job_name,
            "tenant": s.tenant,
            "disposition": s.disposition,
            "incident_id": s.incident_id,
            "triage": {
                "summary": s.judgment.get("summary"),
                "failure_class": s.failure_class,
                "failure_class_probabilities": s.judgment["answers"]["failure_class"].get("probabilities"),
                "actionable": s.actionable,
                "user_impact": s.user_impact,
                "deploy_related": s.deploy_related,
            },
            "evidence_triage_saw": s.judgment.get("state").map(|state| {
                let mut state = state.clone();
                if let Some(map) = state.as_object_mut() {
                    map.remove("triage_policy");
                }
                state
            }),
        }))
    }

    async fn job(&self, job: &str) -> anyhow::Result<Value> {
        let record = self.db.job(job).await?;
        let now = Utc::now();
        let statuses = self.db.status_between(job, now - chrono::Duration::minutes(30), now).await?;
        let snapshot = self.db.job_at(job, now).await?;
        let run = self.flink.active_job(job).await.ok().flatten();
        let parallelism = match &run {
            Some(r) => self.flink.vertices(&r.jid).await.ok().map(|v| job_parallelism(&v)),
            None => None,
        };
        let failures: Vec<Value> = statuses
            .iter()
            .filter_map(|s| {
                let f = s.failure.as_ref()?;
                Some(json!({
                    "at": s.ts,
                    "minutes_ago": minutes_ago(s.ts),
                    "status": s.status,
                    "exception": f.exception_class,
                    "root_cause": f.root_cause_class,
                    "message": f.root_cause_message.as_deref().or(f.message.as_deref()).map(|m| truncate(m, 300)),
                }))
            })
            .rev()
            .take(10)
            .collect();
        Ok(json!({
            "job": record,
            "status_now": snapshot.status.as_ref().map(|s| json!({"status": s.status, "since": s.ts, "restarts": s.restarts})),
            "flink": run.map(|r| json!({"flink_job_id": r.jid, "state": r.state, "parallelism": parallelism})),
            "failures_last_30_minutes": statuses.iter().filter(|s| s.failure.is_some()).count(),
            "latest_failures": failures,
        }))
    }

    async fn deployments(&self, job: &str, limit: u32) -> anyhow::Result<Value> {
        let deployments = self.db.deployments_before(job, Utc::now(), i64::from(limit.clamp(1, 50))).await?;
        Ok(Value::Array(
            deployments
                .iter()
                .map(|d| {
                    json!({
                        "at": d.ts,
                        "minutes_ago": minutes_ago(d.ts),
                        "kind": d.kind,
                        "from_version": d.from_version,
                        "to_version": d.to_version,
                        "commit_sha": d.commit_sha,
                        "author": d.author,
                        "change_summary": d.summary,
                        "extra_args": visible_args(&d.args).into_iter().filter(|a| !a.starts_with("--job=")).collect::<Vec<_>>(),
                        "triggered_by": d.triggered_by,
                        "outcome": d.outcome,
                        "failure_reason": d.failure_reason,
                    })
                })
                .collect(),
        ))
    }

    async fn prometheus(&self, p: PrometheusParams) -> anyhow::Result<Value> {
        let now = Utc::now();
        match p.range_minutes {
            None => {
                let samples = self.prometheus.query(&p.promql, now).await?;
                Ok(Value::Array(
                    samples.into_iter().take(50).map(|s| json!({"labels": s.labels, "value": s.value})).collect(),
                ))
            }
            Some(minutes) => {
                let range = Duration::from_secs(u64::from(minutes.clamp(1, 360)) * 60);
                let step = p.step_seconds.map_or(range / 20, |s| Duration::from_secs(u64::from(s.max(5))));
                let from = now - chrono::Duration::from_std(range)?;
                let series = self.prometheus.query_range(&p.promql, from, now, step).await?;
                Ok(Value::Array(
                    series
                        .into_iter()
                        .take(20)
                        .map(|s| {
                            let points: Vec<Value> = s
                                .points
                                .iter()
                                .map(|(t, v)| json!([t.format("%H:%M:%S").to_string(), (v * 100.0).round() / 100.0]))
                                .collect();
                            json!({"labels": s.labels, "points": points})
                        })
                        .collect(),
                ))
            }
        }
    }

    async fn logs(&self, p: LogsParams) -> anyhow::Result<Value> {
        let now = Utc::now();
        let from = now - chrono::Duration::minutes(i64::from(p.minutes.unwrap_or(DEFAULT_MINUTES).clamp(1, 1440)));
        let level = match p.level.as_deref().map(str::to_ascii_uppercase).as_deref() {
            Some("ERROR") => LogLevel::Error,
            _ => LogLevel::Warn,
        };
        let limit = p.limit.unwrap_or(30).clamp(1, 200) as usize;
        let logs = self.db.logs_between(&p.job, from, now, level, 2000).await?;
        let matching: Vec<Value> = logs
            .iter()
            .filter(|l| p.contains.as_deref().is_none_or(|c| l.message.contains(c)))
            .take(limit)
            .map(|l| {
                json!({
                    "at": l.ts,
                    "level": l.level,
                    "component": l.component,
                    "logger": l.logger,
                    "message": truncate(&l.message, 500),
                    "exception": l.exception_class,
                    "exception_message": l.exception_message.as_deref().map(|m| truncate(m, 500)),
                    "root_cause": l.root_cause_class,
                    "root_cause_message": l.root_cause_message.as_deref().map(|m| truncate(m, 500)),
                    "stack_head": l.stack_head.as_deref().map(|m| truncate(m, 800)),
                })
            })
            .collect();
        Ok(json!({"total_in_window": logs.len(), "shown": matching.len(), "logs": matching}))
    }

    async fn checkpoints(&self, job: &str, minutes: u32) -> anyhow::Result<Value> {
        let now = Utc::now();
        let from = now - chrono::Duration::minutes(i64::from(minutes.clamp(1, 1440)));
        let checkpoints = self.db.checkpoints_between(job, from, now).await?;
        let completed = checkpoints.iter().filter(|c| c.status == platform_model::CheckpointStatus::Completed).count();
        let recent: Vec<Value> = checkpoints
            .iter()
            .rev()
            .take(20)
            .map(|c| {
                json!({
                    "at": c.ts,
                    "id": c.checkpoint_id,
                    "status": c.status,
                    "savepoint": c.is_savepoint,
                    "duration_seconds": c.duration_ms as f64 / 1000.0,
                    "state_size_mb": (c.state_size_bytes as f64 / 1e4).round() / 100.0,
                    "failure_reason": c.failure_reason.as_deref().map(|r| truncate(r, 300)),
                })
            })
            .collect();
        Ok(json!({
            "window_minutes": minutes,
            "completed": completed,
            "failed": checkpoints.len() - completed,
            "latest": recent,
        }))
    }

    /// Reads records from Kafka with a throwaway consumer (no group, no
    /// commits).
    fn sample(&self, p: SampleParams) -> anyhow::Result<Value> {
        let count = i64::from(p.count.unwrap_or(10).clamp(1, 50));
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.kafka_bootstrap_servers)
            .set("group.id", "ops-assistant-sampler")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .create()?;
        let timeout = Duration::from_secs(5);
        let metadata = consumer.fetch_metadata(Some(&p.topic), timeout)?;
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == p.topic)
            .filter(|t| !t.partitions().is_empty())
            .ok_or_else(|| anyhow::anyhow!("no topic named {}", p.topic))?;
        let partitions: Vec<i32> = match p.partition {
            Some(partition) => vec![partition],
            None => topic.partitions().iter().map(|x| x.id()).collect(),
        };
        anyhow::ensure!(p.offset.is_none() || p.partition.is_some(), "an offset needs a partition");
        let mut assignment = TopicPartitionList::new();
        let mut ends = std::collections::HashMap::new();
        for &partition in &partitions {
            let (low, high) = consumer.fetch_watermarks(&p.topic, partition, timeout)?;
            let start = p.offset.unwrap_or(high - count).clamp(low, high);
            let end = (start + count).min(high);
            if start < end {
                assignment.add_partition_offset(&p.topic, partition, Offset::Offset(start))?;
                ends.insert(partition, end);
            }
        }
        consumer.assign(&assignment)?;
        let mut records = Vec::new();
        let deadline = std::time::Instant::now() + timeout;
        while !ends.is_empty() && std::time::Instant::now() < deadline {
            let Some(message) = consumer.poll(Duration::from_millis(200)) else { continue };
            let message = message?;
            let Some(&end) = ends.get(&message.partition()) else { continue };
            if message.offset() >= end {
                ends.remove(&message.partition());
                continue;
            }
            records.push(json!({
                "partition": message.partition(),
                "offset": message.offset(),
                "timestamp": message.timestamp().to_millis().and_then(DateTime::from_timestamp_millis),
                "key": message.key().map(|k| String::from_utf8_lossy(k).into_owned()),
                "value": message.payload().map(|v| truncate(&String::from_utf8_lossy(v), 600)),
            }));
            if message.offset() + 1 >= end {
                ends.remove(&message.partition());
            }
        }
        Ok(json!({"topic": p.topic, "records": records}))
    }

    async fn propose(&self, p: ProposeParams) -> anyhow::Result<Value> {
        let root_id = self.thread.root_id.clone().ok_or_else(|| anyhow::anyhow!("no Mattermost thread to post in"))?;
        let catalogue = self.cicd.catalogue().await?;
        let spec = catalogue
            .jobs
            .get(&p.job)
            .ok_or_else(|| anyhow::anyhow!("{} isn't deployed by the platform's pipeline", p.job))?;
        let (action, params) = match p.remediation {
            Remediation::Redeploy { version, args } => {
                anyhow::ensure!(
                    spec.versions.iter().any(|v| v.version == version),
                    "{} has no version {version}",
                    p.job
                );
                if let Some(bad) = args.iter().find(|a| !ALLOWED_ARGS.contains(&a.as_str())) {
                    anyhow::bail!("{bad} isn't an allowed argument (allowed: {})", ALLOWED_ARGS.join(", "));
                }
                (ActionKind::Redeploy, json!({"version": version, "args": args}))
            }
            Remediation::RollbackToPreviousVersion { to_version } => {
                (ActionKind::RollbackToPreviousVersion, json!({"to_version": to_version}))
            }
            Remediation::ScaleUp { parallelism } => {
                anyhow::ensure!(
                    parallelism <= spec.max_parallelism,
                    "{}'s tenant allows parallelism up to {}",
                    p.job,
                    spec.max_parallelism
                );
                (ActionKind::ScaleUp, json!({"parallelism": parallelism}))
            }
        };
        let signal = match self.thread.signal_id {
            Some(id) => self.db.signal(id).await?,
            None => None,
        };
        let channel = match self.thread.channel.as_deref() {
            Some("platform-incidents") => Channel::PlatformIncidents,
            _ => Channel::PlatformSignals,
        };
        let request = ProposalRequest {
            channel,
            root_id,
            job_name: p.job.clone(),
            signal_id: signal.as_ref().map(|s| s.signal_id),
            incident_id: signal.as_ref().and_then(|s| s.incident_id),
            action,
            params,
            rationale: p.rationale,
            ticket: p.ticket.map(|t| TicketProposal {
                assignment_group: t.assignment_group,
                short_description: t.short_description,
                description: t.description,
            }),
        };
        let response = self.chatops.propose(&request).await?;
        Ok(json!({
            "proposed": true,
            "action_id": response.action_id,
            "note": "An approval card is in the thread. Nothing changes until a person clicks Approve; the platform then carries it out and opens the ticket.",
        }))
    }
}

/// A tool's result as text: the JSON, or the error for Claude to read.
fn reply(result: anyhow::Result<Value>) -> String {
    match result {
        Ok(value) => json_text(value),
        Err(err) => format!("error: {err:#}"),
    }
}

#[tool_router]
impl Platform {
    #[tool(
        description = "A triage signal: the triage agent's verdict on a job's alerts, with its failure class, probabilities and the evidence it saw. Defaults to the signal this thread is about."
    )]
    async fn get_signal(&self, Parameters(p): Parameters<SignalParams>) -> String {
        reply(self.signal(p).await)
    }

    #[tool(
        description = "A job's registry record (tenant, owner, tier, version, maximum parallelism), its status and Flink run now, and its failures in the last 30 minutes."
    )]
    async fn get_job(&self, Parameters(p): Parameters<JobParams>) -> String {
        reply(self.job(&p.job).await)
    }

    #[tool(
        description = "A job's deployments and rollbacks, newest first: versions, commit, author, change summary, extra job arguments, who triggered it and the outcome."
    )]
    async fn get_deployments(&self, Parameters(p): Parameters<DeploymentsParams>) -> String {
        reply(self.deployments(&p.job, p.limit.unwrap_or(10)).await)
    }

    #[tool(
        description = "Run a PromQL query against the platform's Prometheus, now or over the last N minutes. Useful metrics: flink_taskmanager_job_task_operator_pendingRecords (lag), flink_taskmanager_job_task_numRecordsInPerSecond, flink_taskmanager_job_task_backPressuredTimeMsPerSecond, flink_taskmanager_job_task_busyTimeMsPerSecond, flink_jobmanager_job_numRestarts, flink_jobmanager_job_lastCheckpointDuration, flink_jobmanager_job_lastCheckpointSize."
    )]
    async fn query_prometheus(&self, Parameters(p): Parameters<PrometheusParams>) -> String {
        reply(self.prometheus(p).await)
    }

    #[tool(
        description = "A job's warning and error logs, newest first, with exception classes, root causes and stack heads. Filter by level or text."
    )]
    async fn get_logs(&self, Parameters(p): Parameters<LogsParams>) -> String {
        reply(self.logs(p).await)
    }

    #[tool(
        description = "A job's recent checkpoints and savepoints: status, duration, state size and failure reasons."
    )]
    async fn get_checkpoints(&self, Parameters(p): Parameters<CheckpointsParams>) -> String {
        reply(self.checkpoints(&p.job, p.minutes.unwrap_or(DEFAULT_MINUTES)).await)
    }

    #[tool(
        description = "Read records from a Kafka topic without consuming them: the latest ones, or from an offset in one partition (e.g. around an offset an error names). Returns each record's partition, offset, time, key and value."
    )]
    async fn sample_topic(&self, Parameters(p): Parameters<SampleParams>) -> String {
        let platform = self.clone();
        reply(match tokio::task::spawn_blocking(move || platform.sample(p)).await {
            Ok(result) => result,
            Err(err) => Err(err.into()),
        })
    }

    #[tool(description = "A ticket by number (e.g. INC0010001): state, assignment, description and work notes.")]
    async fn get_ticket(&self, Parameters(p): Parameters<TicketParams>) -> String {
        reply(
            async {
                let ticket = self.tickets.find_by_number(&p.number).await?;
                Ok(serde_json::to_value(ticket)?)
            }
            .await,
        )
    }

    #[tool(description = "Search tickets, e.g. the open ones about a job.")]
    async fn search_tickets(&self, Parameters(p): Parameters<SearchTicketsParams>) -> String {
        reply(
            async {
                let mut conditions = Vec::new();
                if let Some(job) = &p.job {
                    conditions.push(format!("cmdb_ci={job}"));
                }
                if p.active.unwrap_or(true) {
                    conditions.push("active=true".to_string());
                }
                conditions.push("ORDERBYDESCsys_created_on".to_string());
                let tickets = self.tickets.query(&conditions.join("^"), p.limit.unwrap_or(10).clamp(1, 50)).await?;
                Ok(Value::Array(
                    tickets
                        .iter()
                        .map(|t| {
                            json!({
                                "number": t.number,
                                "short_description": t.short_description,
                                "state": t.state().map(|s| s.label()),
                                "assignment_group": t.assignment_group,
                                "job": t.cmdb_ci,
                                "opened_at": t.opened_at,
                            })
                        })
                        .collect(),
                ))
            }
            .await,
        )
    }

    #[tool(
        description = "Propose a remediation for a person to approve. This changes nothing: it posts an approval card in the thread, and only a person's Approve carries it out (through the deploy pipeline or Flink's API) and opens the ticket, if you give one. Propose once you have found the root cause, citing the evidence."
    )]
    async fn propose_remediation(&self, Parameters(p): Parameters<ProposeParams>) -> String {
        reply(self.propose(p).await)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Platform {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("platform", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Tools for investigating the streaming platform: triage signals, jobs, deployments, logs, \
                 checkpoints, metrics, Kafka records and tickets. All read-only, except propose_remediation, which \
                 only proposes.",
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_flags_stay_out_of_the_evidence() {
        let args = vec!["--job=x".to_string(), "--fault=state-leak".to_string(), "--on-deser-error=dlq".to_string()];
        assert_eq!(visible_args(&args), [&args[0], &args[2]]);
        assert_eq!(truncate("abcdef", 3), "abc…");
    }
}
