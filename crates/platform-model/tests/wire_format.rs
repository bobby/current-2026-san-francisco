//! The JSON wire format that ShadowTraffic's fleet configs must produce.

use platform_model::{CheckpointEvent, CheckpointStatus, JobState, JobStatusEvent};

#[test]
fn job_status_event_round_trips() {
    let json = r#"{
        "event_id": "0b6f5e1c-6a4e-5c1e-9d6f-1f2e3d4c5b6a",
        "ts": "2026-10-20T14:03:07.250Z",
        "job_name": "payments-enrichment",
        "flink_job_id": "3bba68a3dfd5f152ae18e85dc6131f14",
        "status": "RESTARTING",
        "previous_status": "RUNNING",
        "restarts": 3,
        "failure": {
            "exception_class": "java.io.IOException",
            "message": "Failed to deserialize consumer record due to",
            "root_cause_class": "java.lang.NullPointerException",
            "root_cause_message": null,
            "stack_head": "java.io.IOException: Failed to deserialize consumer record due to",
            "task": "Source: payments_raw[1] (1/1)"
        }
    }"#;
    let event: JobStatusEvent = serde_json::from_str(json).unwrap();
    assert_eq!(event.status, JobState::Restarting);
    assert_eq!(event.failure.as_ref().unwrap().root_cause_class.as_deref(), Some("java.lang.NullPointerException"));
    let again: JobStatusEvent = serde_json::from_value(serde_json::to_value(&event).unwrap()).unwrap();
    assert_eq!(event, again);
}

#[test]
fn checkpoint_event_uses_snake_case_status() {
    let event: CheckpointEvent = serde_json::from_value(serde_json::json!({
        "event_id": "0b6f5e1c-6a4e-5c1e-9d6f-1f2e3d4c5b6a",
        "ts": "2026-10-20T14:03:07Z",
        "job_name": "positions-aggregator",
        "flink_job_id": "e28f0689f53ec7e3b011a25a987f94e5",
        "checkpoint_id": 42,
        "status": "completed",
        "is_savepoint": false,
        "trigger_ts": "2026-10-20T14:03:06Z",
        "duration_ms": 812,
        "checkpointed_size_bytes": 1048576,
        "state_size_bytes": 4194304,
        "failure_reason": null
    }))
    .unwrap();
    assert_eq!(event.status, CheckpointStatus::Completed);
}

#[test]
fn event_ids_are_deterministic() {
    let a = platform_model::event_id(&["checkpoint", "abc", "42"]);
    assert_eq!(a, platform_model::event_id(&["checkpoint", "abc", "42"]));
    assert_ne!(a, platform_model::event_id(&["checkpoint", "abc", "43"]));
}
