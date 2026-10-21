//! The collector's HTTP endpoints:
//!
//! - `POST /alertmanager`: Alertmanager's webhook → `platform.alerts`.
//! - `POST /logs`: Flink JSON log lines from Vector (newline-delimited) →
//!   `platform.logs`, with the job name added.
//! - `GET /health`.

use std::{collections::HashMap, net::SocketAddr};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use chrono::{DateTime, Datelike, Utc};
use platform_clients::stack_trace;
use platform_model::{AlertEvent, AlertStatus, LogEvent, LogLevel, event_id, topics};
use serde::Deserialize;
use tracing::{info, warn};

use crate::{jobs::JobNames, publisher::Publisher};

/// Longest log message kept; Flink's task names alone can run to kilobytes.
const MAX_MESSAGE_CHARS: usize = 2000;

#[derive(Clone)]
struct AppState {
    publisher: Publisher,
    jobs: JobNames,
}

pub async fn serve(addr: SocketAddr, publisher: Publisher, jobs: JobNames) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/alertmanager", post(alertmanager))
        .route("/logs", post(logs))
        .with_state(AppState { publisher, jobs });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "HTTP listening");
    axum::serve(listener, app).await?;
    Ok(())
}

type HandlerResult = Result<StatusCode, (StatusCode, String)>;

fn internal(err: anyhow::Error) -> (StatusCode, String) {
    warn!(%err, "request failed");
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

/// Alertmanager's webhook payload (version 4).
#[derive(Debug, Deserialize)]
struct Webhook {
    alerts: Vec<WebhookAlert>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WebhookAlert {
    status: AlertStatus,
    labels: HashMap<String, String>,
    #[serde(default)]
    annotations: HashMap<String, String>,
    starts_at: DateTime<Utc>,
    ends_at: Option<DateTime<Utc>>,
    #[serde(rename = "generatorURL")]
    generator_url: Option<String>,
    fingerprint: String,
}

async fn alertmanager(State(state): State<AppState>, Json(webhook): Json<Webhook>) -> HandlerResult {
    for alert in webhook.alerts {
        let event = alert_event(&state.jobs, alert).await;
        let key = event.job_name.clone().unwrap_or_else(|| event.alertname.clone());
        state.publisher.publish(topics::ALERTS, &key, &event).await.map_err(internal)?;
    }
    Ok(StatusCode::OK)
}

async fn alert_event(jobs: &JobNames, alert: WebhookAlert) -> AlertEvent {
    // Alertmanager sends year 1 as the end of an alert that is still firing.
    let ends_at = alert.ends_at.filter(|t| t.year() > 1);
    let ts = match alert.status {
        AlertStatus::Firing => alert.starts_at,
        AlertStatus::Resolved => ends_at.unwrap_or_else(Utc::now),
    };
    let job_name = jobs.for_alert_labels(&alert.labels).await;
    let tenant = match (alert.labels.get("tenant"), &job_name) {
        (Some(tenant), _) => Some(tenant.clone()),
        (None, Some(job)) => jobs.tenant(job).await,
        (None, None) => None,
    };
    let status = match alert.status {
        AlertStatus::Firing => "firing",
        AlertStatus::Resolved => "resolved",
    };
    AlertEvent {
        // One event per alert per transition, however often Alertmanager repeats it.
        event_id: event_id(&["alert", &alert.fingerprint, status, &alert.starts_at.to_rfc3339()]),
        ts,
        status: alert.status,
        alertname: alert.labels.get("alertname").cloned().unwrap_or_default(),
        fingerprint: alert.fingerprint,
        severity: alert.labels.get("severity").cloned(),
        job_name,
        tenant,
        starts_at: alert.starts_at,
        ends_at,
        labels: alert.labels.into_iter().collect(),
        annotations: alert.annotations.into_iter().collect(),
        generator_url: alert.generator_url,
    }
}

/// One line of Flink's JSON log (flink/conf/log-template.json).
#[derive(Debug, Deserialize)]
struct FlinkLogLine {
    #[serde(rename = "@timestamp")]
    timestamp: DateTime<Utc>,
    level: LogLevel,
    logger: String,
    #[serde(default)]
    component: String,
    #[serde(default)]
    host: String,
    message: String,
    #[serde(default)]
    mdc: HashMap<String, String>,
    #[serde(rename = "error.type")]
    error_type: Option<String>,
    #[serde(rename = "error.message")]
    error_message: Option<String>,
    #[serde(rename = "error.stack_trace")]
    error_stack_trace: Option<String>,
}

async fn logs(State(state): State<AppState>, body: String) -> HandlerResult {
    let mut unresolved = 0;
    for line in body.lines().filter(|l| !l.trim().is_empty()) {
        let log: FlinkLogLine = match serde_json::from_str(line) {
            Ok(log) => log,
            Err(err) => {
                warn!(%err, "skipping a log line that isn't Flink JSON");
                continue;
            }
        };
        let Some(flink_job_id) = log.mdc.get("flink-job-id").cloned() else { continue };
        let Some(job_name) = state.jobs.by_flink_id(&flink_job_id).await else {
            unresolved += 1;
            continue;
        };
        let event = log_event(log, job_name, flink_job_id);
        state.publisher.publish(topics::LOGS, &event.job_name, &event).await.map_err(internal)?;
    }
    if unresolved > 0 {
        warn!(unresolved, "dropped log lines from jobs Flink no longer knows");
    }
    Ok(StatusCode::OK)
}

fn log_event(log: FlinkLogLine, job_name: String, flink_job_id: String) -> LogEvent {
    let summary = log.error_stack_trace.as_deref().map(stack_trace::summarize).unwrap_or_default();
    let message: String = log.message.chars().take(MAX_MESSAGE_CHARS).collect();
    LogEvent {
        event_id: event_id(&["log", &log.host, &log.timestamp.to_rfc3339(), &log.logger, &message]),
        ts: log.timestamp,
        job_name,
        flink_job_id: Some(flink_job_id),
        level: log.level,
        component: log.component,
        host: log.host,
        logger: log.logger,
        message,
        exception_class: log.error_type,
        exception_message: log.error_message,
        root_cause_class: summary.root_cause_class,
        root_cause_message: summary.root_cause_message,
        stack_head: summary.head,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flink_log_line_becomes_a_log_event() {
        let line = r#"{"@timestamp":"2026-10-21T06:13:17.323Z","level":"WARN","logger":"org.apache.flink.runtime.taskmanager.Task","thread":"t","component":"taskmanager","host":"40a376df126b","message":"Source: payments_raw[1] switched from RUNNING to FAILED with failure cause:","mdc":{"flink-job-id":"3bba68a3"},"error.type":"java.io.IOException","error.message":"Failed to deserialize consumer record due to","error.stack_trace":"java.io.IOException: Failed to deserialize consumer record due to\n\tat a.b(C.java:1)\nCaused by: java.lang.NullPointerException\n\tat d.e(F.java:2)"}"#;
        let log: FlinkLogLine = serde_json::from_str(line).unwrap();
        let event = log_event(log, "payments-enrichment".into(), "3bba68a3".into());
        assert_eq!(event.level, LogLevel::Warn);
        assert_eq!(event.component, "taskmanager");
        assert_eq!(event.exception_class.as_deref(), Some("java.io.IOException"));
        assert_eq!(event.root_cause_class.as_deref(), Some("java.lang.NullPointerException"));
        assert!(event.stack_head.unwrap().starts_with("java.io.IOException"));
    }

    #[test]
    fn alertmanager_payload_parses() {
        let webhook: Webhook = serde_json::from_value(serde_json::json!({
            "version": "4", "status": "firing", "receiver": "collector", "groupLabels": {}, "commonLabels": {},
            "commonAnnotations": {}, "externalURL": "http://alertmanager:9093", "groupKey": "{}", "truncatedAlerts": 0,
            "alerts": [{
                "status": "firing",
                "labels": {"alertname": "FlinkJobRestarting", "job_name": "payments_enrichment", "severity": "warning"},
                "annotations": {"summary": "restarting"},
                "startsAt": "2026-10-21T06:13:17.323Z", "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": "http://prometheus:9090/graph", "fingerprint": "c0ffee"
            }]
        }))
        .unwrap();
        assert_eq!(webhook.alerts[0].status, AlertStatus::Firing);
        assert_eq!(webhook.alerts[0].ends_at.unwrap().year(), 1);
    }
}
