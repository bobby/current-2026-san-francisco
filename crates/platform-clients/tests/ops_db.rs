//! Ops DB writes and point-in-time queries, against Postgres. Each test gets
//! its own throwaway database. Run with `make rust-test`, which points
//! DATABASE_URL at the local stack's Postgres.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use platform_clients::ops_db::OpsDb;
use platform_model::*;
use sqlx::PgPool;

fn t(seconds: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-10-20T14:00:00Z").unwrap().with_timezone(&Utc) + Duration::seconds(seconds)
}

fn checkpoint(id: u64, at: i64, status: CheckpointStatus) -> CheckpointEvent {
    CheckpointEvent {
        event_id: event_id(&["checkpoint", "jid-1", &id.to_string()]),
        ts: t(at),
        job_name: "positions-aggregator".into(),
        flink_job_id: "jid-1".into(),
        checkpoint_id: id,
        status,
        is_savepoint: false,
        trigger_ts: t(at - 1),
        duration_ms: 800 + id * 100,
        checkpointed_size_bytes: id * 1_000_000,
        state_size_bytes: id * 4_000_000,
        failure_reason: (status == CheckpointStatus::Failed).then(|| "Checkpoint expired before completing.".into()),
        external_path: (status == CheckpointStatus::Completed)
            .then(|| format!("s3://flink/checkpoints/jid-1/chk-{id}")),
    }
}

fn deployment(version: &str, at: i64) -> Deployment {
    Deployment {
        event_id: event_id(&["deploy", version]),
        ts: t(at),
        job_name: "positions-aggregator".into(),
        tenant: "risk-platform".into(),
        kind: DeploymentKind::Deploy,
        from_version: None,
        to_version: version.into(),
        commit_sha: "abc1234".into(),
        author: "dana".into(),
        summary: "Track recent fills".into(),
        args: vec!["--job=positions-aggregator".into()],
        triggered_by: "director".into(),
        failure_reason: None,
        outcome: DeploymentOutcome::Succeeded,
        flink_job_id: Some("jid-1".into()),
        restored_from: None,
    }
}

fn status(at: i64, status: JobState, failure: Option<Failure>) -> JobStatusEvent {
    JobStatusEvent {
        event_id: event_id(&["status", &at.to_string()]),
        ts: t(at),
        job_name: "positions-aggregator".into(),
        flink_job_id: "jid-1".into(),
        status,
        previous_status: Some(JobState::Running),
        restarts: 1,
        failure,
    }
}

fn log(at: i64, level: LogLevel) -> LogEvent {
    LogEvent {
        event_id: event_id(&["log", &at.to_string()]),
        ts: t(at),
        job_name: "positions-aggregator".into(),
        flink_job_id: Some("jid-1".into()),
        level,
        component: "taskmanager".into(),
        host: "tm-1".into(),
        logger: "org.apache.flink.runtime.taskmanager.Task".into(),
        message: format!("message at {at}"),
        exception_class: None,
        exception_message: None,
        root_cause_class: None,
        root_cause_message: None,
        stack_head: None,
    }
}

#[sqlx::test(migrator = "platform_clients::ops_db::MIGRATOR")]
#[ignore = "needs Postgres: run `make rust-test`"]
async fn checkpoint_history_before_a_time(pool: PgPool) {
    let db = OpsDb::from_pool(pool);
    for (id, at) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
        db.insert_checkpoint(&checkpoint(id, at, CheckpointStatus::Completed)).await.unwrap();
    }
    // Re-delivered events are stored once.
    db.insert_checkpoint(&checkpoint(2, 20, CheckpointStatus::Completed)).await.unwrap();

    let before = db.checkpoints_before("positions-aggregator", t(35), 10).await.unwrap();
    assert_eq!(before.iter().map(|c| c.checkpoint_id).collect::<Vec<_>>(), [3, 2, 1]);
    assert_eq!(before[0], checkpoint(3, 30, CheckpointStatus::Completed));

    let limited = db.checkpoints_before("positions-aggregator", t(100), 2).await.unwrap();
    assert_eq!(limited.iter().map(|c| c.checkpoint_id).collect::<Vec<_>>(), [4, 3]);
    assert!(db.checkpoints_before("other-job", t(100), 10).await.unwrap().is_empty());

    let between = db.checkpoints_between("positions-aggregator", t(20), t(40)).await.unwrap();
    assert_eq!(between.iter().map(|c| c.checkpoint_id).collect::<Vec<_>>(), [2, 3]);
}

#[sqlx::test(migrator = "platform_clients::ops_db::MIGRATOR")]
#[ignore = "needs Postgres: run `make rust-test`"]
async fn job_snapshot_at_a_time(pool: PgPool) {
    let db = OpsDb::from_pool(pool);
    db.insert_deployment(&deployment("v1", 0)).await.unwrap();
    db.insert_deployment(&deployment("v2", 100)).await.unwrap();
    db.insert_checkpoint(&checkpoint(1, 10, CheckpointStatus::Completed)).await.unwrap();
    db.insert_checkpoint(&checkpoint(2, 20, CheckpointStatus::Failed)).await.unwrap();
    db.insert_checkpoint(&checkpoint(3, 30, CheckpointStatus::Failed)).await.unwrap();
    db.insert_checkpoint(&checkpoint(4, 110, CheckpointStatus::Completed)).await.unwrap();
    let npe = Failure {
        exception_class: "java.io.IOException".into(),
        message: Some("Failed to deserialize consumer record due to".into()),
        root_cause_class: Some("java.lang.NullPointerException".into()),
        root_cause_message: None,
        stack_head: Some("java.io.IOException: Failed to deserialize consumer record due to".into()),
        task: None,
    };
    db.insert_job_status(&status(25, JobState::Restarting, Some(npe.clone()))).await.unwrap();
    db.insert_job_status(&status(105, JobState::Running, None)).await.unwrap();

    let at_50 = db.job_at("positions-aggregator", t(50)).await.unwrap();
    assert_eq!(at_50.deployment.unwrap().to_version, "v1");
    assert_eq!(at_50.last_completed_checkpoint.unwrap().checkpoint_id, 1);
    assert_eq!(at_50.failed_checkpoints_since, 2);
    let status_50 = at_50.status.unwrap();
    assert_eq!(status_50.status, JobState::Restarting);
    assert_eq!(status_50.failure, Some(npe));

    let at_120 = db.job_at("positions-aggregator", t(120)).await.unwrap();
    assert_eq!(at_120.deployment.unwrap().to_version, "v2");
    assert_eq!(at_120.last_completed_checkpoint.unwrap().checkpoint_id, 4);
    assert_eq!(at_120.failed_checkpoints_since, 0);
    assert_eq!(at_120.status.unwrap().status, JobState::Running);

    let restore = db.latest_restore_point("positions-aggregator").await.unwrap().unwrap();
    assert_eq!(restore.path, "s3://flink/checkpoints/jid-1/chk-4");
    // Once the run finishes (stop-with-savepoint), Flink deletes its
    // checkpoints; a deployment's restore point is the newest left.
    let mut redeploy = deployment("v3", 115);
    redeploy.restored_from = Some("s3://flink/savepoints/savepoint-jid1-abc".into());
    db.insert_deployment(&redeploy).await.unwrap();
    db.insert_job_status(&status(112, JobState::Finished, None)).await.unwrap();
    let restore = db.latest_restore_point("positions-aggregator").await.unwrap().unwrap();
    assert_eq!((restore.path.as_str(), restore.ts), ("s3://flink/savepoints/savepoint-jid1-abc", t(115)));
    assert_eq!(
        db.deployments_before("positions-aggregator", t(200), 1).await.unwrap()[0].args,
        ["--job=positions-aggregator"]
    );

    let before_any = db.job_at("positions-aggregator", t(-10)).await.unwrap();
    assert!(before_any.deployment.is_none() && before_any.status.is_none());

    assert_eq!(db.deployments_before("positions-aggregator", t(100), 5).await.unwrap().len(), 1);
    assert_eq!(db.status_between("positions-aggregator", t(0), t(200)).await.unwrap().len(), 3);
}

#[sqlx::test(migrator = "platform_clients::ops_db::MIGRATOR")]
#[ignore = "needs Postgres: run `make rust-test`"]
async fn logs_by_level_and_retention(pool: PgPool) {
    let db = OpsDb::from_pool(pool);
    db.insert_log(&log(10, LogLevel::Info)).await.unwrap();
    db.insert_log(&log(20, LogLevel::Warn)).await.unwrap();
    db.insert_log(&log(30, LogLevel::Error)).await.unwrap();

    let warnings = db.logs_between("positions-aggregator", t(0), t(100), LogLevel::Warn, 10).await.unwrap();
    assert_eq!(warnings.iter().map(|l| l.level).collect::<Vec<_>>(), [LogLevel::Error, LogLevel::Warn]);

    assert_eq!(db.delete_logs_before(t(25)).await.unwrap(), 2);
    let left = db.logs_between("positions-aggregator", t(0), t(100), LogLevel::Trace, 10).await.unwrap();
    assert_eq!(left, [log(30, LogLevel::Error)]);
}

#[sqlx::test(migrator = "platform_clients::ops_db::MIGRATOR")]
#[ignore = "needs Postgres: run `make rust-test`"]
async fn jobs_alerts_signals_and_actions(pool: PgPool) {
    let db = OpsDb::from_pool(pool);
    let mut job = JobRecord {
        job_name: "payments-enrichment".into(),
        tenant: "payments-eng".into(),
        owner: "@payments-eng".into(),
        tier: Tier::Tier1,
        kind: JobKind::Java,
        current_version: "v7".into(),
        flink_job_id: Some("jid-7".into()),
        max_parallelism: 4,
        deployed_at: t(0),
        first_deployed_at: t(-86_400),
    };
    db.upsert_job(&job).await.unwrap();
    job.current_version = "v8".into();
    db.upsert_job(&job).await.unwrap();
    assert_eq!(db.job("payments-enrichment").await.unwrap(), Some(job));
    db.delete_job("payments-enrichment").await.unwrap();
    assert_eq!(db.job("payments-enrichment").await.unwrap(), None);

    let alert = AlertEvent {
        event_id: event_id(&["alert", "fp1", "firing"]),
        ts: t(5),
        status: AlertStatus::Firing,
        alertname: "FlinkJobRestarting".into(),
        fingerprint: "fp1".into(),
        severity: Some("warning".into()),
        job_name: Some("payments-enrichment".into()),
        tenant: None,
        starts_at: t(5),
        ends_at: None,
        labels: BTreeMap::from([("alertname".into(), "FlinkJobRestarting".into())]),
        annotations: BTreeMap::new(),
        generator_url: None,
    };
    db.insert_alert(&alert).await.unwrap();
    db.insert_alert(&alert).await.unwrap();
    assert_eq!(
        db.alerts_between(Some("payments-enrichment"), t(0), t(10)).await.unwrap(),
        std::slice::from_ref(&alert)
    );
    assert_eq!(db.alerts_between(None, t(0), t(10)).await.unwrap().len(), 1);

    let signal = Signal {
        signal_id: event_id(&["signal", "1"]),
        ts: t(6),
        job_name: "payments-enrichment".into(),
        tenant: "payments-eng".into(),
        alert_event_ids: vec![alert.event_id],
        disposition: Disposition::Raise,
        incident_id: Some(event_id(&["incident", "1"])),
        failure_class: Some(FailureClass::CodeRegression),
        actionable: Some(0.97),
        user_impact: Some(0.9),
        deploy_related: Some(0.93),
        deployment_event_id: None,
        judgment: serde_json::json!({"answers": {"actionable": 0.97}}),
    };
    db.insert_signal(&signal).await.unwrap();
    assert_eq!(
        db.signals_between(Some("payments-enrichment"), t(0), t(10)).await.unwrap(),
        std::slice::from_ref(&signal)
    );
    assert!(db.signals_between(None, t(7), t(10)).await.unwrap().is_empty());
    let action = ActionEvent {
        event_id: event_id(&["action", "1", "proposed"]),
        ts: t(7),
        action_id: event_id(&["action", "1"]),
        job_name: "payments-enrichment".into(),
        incident_id: signal.incident_id,
        signal_id: Some(signal.signal_id),
        actor: "operator".into(),
        action: ActionKind::RollbackToPreviousVersion,
        phase: ActionPhase::Proposed,
        params: serde_json::json!({"to_version": "v7"}),
        rationale: None,
        ticket: None,
    };
    db.insert_action(&action).await.unwrap();
    let (disposition, phase): (String, String) =
        sqlx::query_as("SELECT s.disposition, a.phase FROM signals s JOIN actions a ON a.incident_id = s.incident_id")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!((disposition.as_str(), phase.as_str()), ("raise", "proposed"));
    assert_eq!(db.actions_between("payments-enrichment", t(0), t(10)).await.unwrap(), std::slice::from_ref(&action));
    assert!(db.actions_between("payments-enrichment", t(8), t(10)).await.unwrap().is_empty());
}
