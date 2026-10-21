//! Renders the simulated jobs' metrics in Prometheus text format, with the
//! names and label sets Flink's Prometheus reporter uses for real jobs
//! (docs/flink-metrics.md), including its label-value rewriting.

use std::fmt::Write;

use chrono::{DateTime, Utc};

use crate::sim::{SimJob, flink_id};

/// Rewrites a label value as Flink's reporter does: every character outside
/// `[A-Za-z0-9:_]` becomes `_`.
pub fn label_value(value: &str) -> String {
    value.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' }).collect()
}

type Labels = Vec<(&'static str, String)>;

#[derive(Default)]
struct Families {
    /// name → series, kept in first-seen order so output is stable.
    order: Vec<&'static str>,
    series: std::collections::HashMap<&'static str, Vec<(Labels, f64)>>,
}

impl Families {
    fn add(&mut self, name: &'static str, labels: &Labels, value: f64) {
        let entry = self.series.entry(name).or_insert_with(|| {
            self.order.push(name);
            Vec::new()
        });
        entry.push((labels.clone(), value));
    }

    fn render(self) -> String {
        let mut out = String::new();
        for name in &self.order {
            let _ = writeln!(out, "# TYPE {name} gauge");
            for (labels, value) in &self.series[name] {
                let labels: Vec<String> = labels.iter().map(|(k, v)| format!("{k}=\"{v}\"")).collect();
                let _ = writeln!(out, "{name}{{{}}} {value}", labels.join(","));
            }
        }
        out
    }
}

/// Renders every job last updated after `stale_before`.
pub fn render<'a>(jobs: impl Iterator<Item = &'a SimJob>, now: DateTime<Utc>, stale_before: DateTime<Utc>) -> String {
    let mut families = Families::default();
    let mut tms = std::collections::BTreeMap::new();
    for job in jobs.filter(|j| j.last_update >= stale_before) {
        job_metrics(&mut families, job, now);
        tms.entry(tm_id(job)).or_insert_with(|| job.clone());
    }
    // TaskManager process metrics, once per TaskManager.
    for (tm, job) in tms {
        let labels: Labels = vec![("host", tm_host(&job)), ("tm_id", tm)];
        let heap_max = job.info.heap_max_bytes;
        // Heap follows the job's state, as with heap state backends (F4).
        let used = (job.heap_used_ratio * heap_max + job.state_size_bytes * 1.5).min(heap_max * 0.97);
        families.add("flink_taskmanager_Status_JVM_Memory_Heap_Used", &labels, used.round());
        families.add("flink_taskmanager_Status_JVM_Memory_Heap_Max", &labels, heap_max);
        let gc = if used > heap_max * 0.85 { 250.0 } else { 3.0 };
        families.add("flink_taskmanager_Status_JVM_GarbageCollector_G1_Old_Generation_TimeMsPerSecond", &labels, gc);
    }
    families.render()
}

fn tm_host(job: &SimJob) -> String {
    label_value(&format!("10.44.{}.{}", job.cluster_number(), job.host_number()))
}

fn tm_id(job: &SimJob) -> String {
    let suffix = &flink_id(&["fleet-tm-id", &job.info.job_name])[..6];
    format!("{}:6122_{suffix}", tm_host(job))
}

fn job_metrics(families: &mut Families, job: &SimJob, now: DateTime<Utc>) {
    let since_ms = (now - job.since).num_milliseconds().max(0) as f64;
    let jm: Labels = vec![
        ("host", label_value(&job.info.cluster)),
        ("job_id", job.flink_job_id.clone()),
        ("job_name", label_value(&job.info.job_name)),
    ];
    let (uptime, down) = if job.running { (since_ms, 0.0) } else { (0.0, since_ms) };
    families.add("flink_jobmanager_job_uptime", &jm, uptime);
    families.add("flink_jobmanager_job_downtime", &jm, down);
    families.add("flink_jobmanager_job_restartingTime", &jm, down);
    families.add("flink_jobmanager_job_numRestarts", &jm, job.restarts as f64);
    families.add("flink_jobmanager_job_numberOfCompletedCheckpoints", &jm, job.completed_checkpoints as f64);
    families.add("flink_jobmanager_job_numberOfFailedCheckpoints", &jm, job.failed_checkpoints as f64);
    let (duration, size, full, completed) = match job.last_checkpoint {
        Some((d, s, f, at)) => (d, s, f, at.timestamp_millis() as f64),
        None => (-1.0, -1.0, -1.0, -1.0),
    };
    families.add("flink_jobmanager_job_lastCheckpointDuration", &jm, duration.round());
    families.add("flink_jobmanager_job_lastCheckpointSize", &jm, size.round());
    families.add("flink_jobmanager_job_lastCheckpointFullSize", &jm, full.round());
    families.add("flink_jobmanager_job_lastCheckpointCompletedTimestamp", &jm, completed);

    // Task and source-operator metrics, per subtask, as Flink reports them.
    let parallelism = job.info.parallelism.max(1);
    let task_id = flink_id(&["fleet-task", &job.info.job_name]);
    let consumed = job.consumed_per_sec();
    let lag = job.pending_records + job.backlog;
    for subtask in 0..parallelism {
        let task: Labels = vec![
            ("host", tm_host(job)),
            ("job_id", job.flink_job_id.clone()),
            ("job_name", label_value(&job.info.job_name)),
            ("subtask_index", subtask.to_string()),
            ("task_attempt_id", format!("{}_{task_id}_{subtask}_{}", job.flink_job_id, job.restarts)),
            ("task_attempt_num", job.restarts.to_string()),
            ("task_id", task_id.clone()),
            ("task_name", label_value(&job.task_name())),
            ("tm_id", tm_id(job)),
        ];
        let share = 1.0 / parallelism as f64;
        let (busy, backpressure) = match (job.running, job.backlog > 0.0) {
            (false, _) => (0.0, 0.0),
            (true, true) => (980.0, job.backpressured_ms_per_sec.max(400.0)),
            (true, false) => (job.busy_ms_per_sec, job.backpressured_ms_per_sec),
        };
        families.add("flink_taskmanager_job_task_backPressuredTimeMsPerSecond", &task, backpressure.round());
        families.add("flink_taskmanager_job_task_busyTimeMsPerSecond", &task, busy.round());
        families.add("flink_taskmanager_job_task_numRecordsInPerSecond", &task, round2(consumed * share));
        families.add("flink_taskmanager_job_task_numRecordsOutPerSecond", &task, round2(consumed * share));

        let mut operator = task.clone();
        operator.insert(7, ("operator_id", task_id.clone()));
        operator.insert(8, ("operator_name", label_value(&job.source_name())));
        families.add("flink_taskmanager_job_task_operator_pendingRecords", &operator, (lag * share).round());
        families.add(
            "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max",
            &operator,
            (lag * share / 3.0).round(),
        );
        for partition in 0..3 {
            let mut offsets = operator.clone();
            offsets.push(("partition", (subtask * 3 + partition).to_string()));
            offsets.push(("topic", label_value(&job.info.input_topic)));
            let offset = job.offsets.get((subtask * 3 + partition) as usize).copied().unwrap_or_default();
            families.add(
                "flink_taskmanager_job_task_operator_KafkaSourceReader_topic_partition_currentOffset",
                &offsets,
                offset.round(),
            );
        }
    }
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::Fleet;
    use platform_model::fleet::{FleetEvent, FleetEventKind};

    #[test]
    fn rewrites_like_flink() {
        assert_eq!(label_value("Source: market_data[24] -> Calc[25]"), "Source:_market_data_24_____Calc_25_");
        assert_eq!(label_value("10.44.3.57"), "10_44_3_57");
    }

    #[test]
    fn renders_real_flink_names_and_labels() {
        let mut fleet = Fleet::default();
        let event = FleetEvent {
            job_name: "fx-pricing-quote-router".into(),
            tenant: "fx-pricing".into(),
            owner: "@fx-pricing".into(),
            tier: platform_model::Tier::Tier2,
            kind: platform_model::JobKind::Sql,
            max_parallelism: 2,
            parallelism: 1,
            cluster: "flink-04".into(),
            input_topic: "fx-pricing.quotes".into(),
            sink_table: "fx_pricing.quotes".into(),
            first_deployed_at: "2025-01-01T00:00:00Z".parse().unwrap(),
            version: "1.2.3".into(),
            author: "kenji.ito".into(),
            base_rate: 50.0,
            heap_max_bytes: 1e9,
            event: FleetEventKind::Metrics {
                records_in_per_sec: 50.0,
                busy_ms_per_sec: 120.0,
                backpressured_ms_per_sec: 5.0,
                pending_records: 40.0,
                heap_used_ratio: 0.4,
            },
        };
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        fleet.apply(event, now);
        let text = render(fleet.jobs.values(), now, now);
        let job = &fleet.jobs["fx-pricing-quote-router"];
        assert!(text.contains(&format!(
            "flink_jobmanager_job_numRestarts{{host=\"flink_04\",job_id=\"{}\",job_name=\"fx_pricing_quote_router\"}} 0",
            job.flink_job_id
        )));
        let pending =
            text.lines().find(|l| l.starts_with("flink_taskmanager_job_task_operator_pendingRecords{")).unwrap();
        for label in [
            "host",
            "job_id",
            "job_name",
            "operator_id",
            "operator_name",
            "subtask_index",
            "task_attempt_id",
            "task_attempt_num",
            "task_id",
            "task_name",
            "tm_id",
        ] {
            assert!(pending.contains(&format!("{label}=\"")), "{label} missing from {pending}");
        }
        assert!(pending.ends_with(" 40"));
        assert!(text.contains("# TYPE flink_taskmanager_Status_JVM_Memory_Heap_Used gauge"));
    }
}
