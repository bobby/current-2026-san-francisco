//! Ticket notes and card text: code templates over the facts and Jev's
//! answers. Jev never writes text.

use platform_model::{FailureClass, Signal};
use serde_json::Value;

use crate::{
    facts::Health,
    guardrails::{Assessment, Plan, Team},
    questions::Decision,
};

pub fn percent(p: f64) -> String {
    format!("{:.0}%", p * 100.0)
}

pub fn class_label(class: Option<FailureClass>) -> &'static str {
    match class {
        Some(FailureClass::CodeRegression) => "code regression",
        Some(FailureClass::BadInputData) => "bad input data",
        Some(FailureClass::ResourceSaturation) => "resource saturation",
        Some(FailureClass::StateGrowth) => "state growth",
        Some(FailureClass::SchemaMismatch) => "schema mismatch",
        Some(FailureClass::PlatformInfrastructure) => "platform infrastructure",
        Some(FailureClass::TransientOrExpected) => "transient",
        Some(FailureClass::Unknown) | None => "unknown problem",
    }
}

pub fn team_name(team: Team, owner: &str) -> String {
    match team {
        Team::Owner => owner.to_string(),
        Team::Platform => "the platform team".to_string(),
    }
}

/// The owner's handle as a ServiceNow assignment group: `@payments-eng` →
/// `payments-eng`.
pub fn assignment_group(team: Team, owner: &str) -> String {
    match team {
        Team::Owner => owner.trim_start_matches('@').to_string(),
        Team::Platform => "platform-operations".to_string(),
    }
}

pub fn summary(signal: &Signal) -> String {
    signal.judgment.get("summary").and_then(Value::as_str).unwrap_or_default().to_string()
}

pub fn title(signal: &Signal) -> String {
    format!("{}: {}", signal.job_name, class_label(signal.failure_class))
}

/// The ticket's description: what triage raised.
pub fn description(signal: &Signal, class_probability: f64) -> String {
    format!(
        "Triage raised {} (tenant {}) as {} ({}), actionable {}, user impact {}.\n{}\nSignal {}, incident {}.",
        signal.job_name,
        signal.tenant,
        class_label(signal.failure_class),
        percent(class_probability),
        signal.actionable.map_or("—".into(), percent),
        signal.user_impact.map_or("—".into(), percent),
        summary(signal),
        signal.signal_id,
        signal.incident_id.map_or("—".into(), |i| i.to_string()),
    )
}

pub fn decided(decision: &Decision) -> String {
    let mut others: Vec<(&String, &f64)> =
        decision.probabilities.iter().filter(|(k, _)| k.as_str() != decision.remediation.name()).collect();
    others.sort_by(|a, b| b.1.total_cmp(a.1));
    let others: Vec<String> =
        others.iter().take(2).map(|(k, p)| format!("{} {}", k.replace('_', " "), percent(**p))).collect();
    format!(
        "Jev recommends: {} ({}). Next: {}.",
        decision.remediation.label(),
        percent(decision.probability),
        others.join(", ")
    )
}

pub fn guardrails(assessment: &Assessment, decision: &Decision) -> String {
    let checks: Vec<String> =
        assessment.checks.iter().map(|c| format!("{} {}", if c.passed { "✓" } else { "✗" }, c.detail)).collect();
    match &assessment.verdict {
        crate::guardrails::Verdict::Act(plan) => {
            format!("Guardrails passed: {}. Acting: {}.", checks.join("; "), plan.describe())
        }
        _ if checks.is_empty() => format!("No automatic action: Jev recommends to {}.", decision.remediation.label()),
        _ => format!("Guardrails stopped \"{}\": {}.", decision.remediation.label(), checks.join("; ")),
    }
}

/// One line about a job's health.
pub fn health(h: &Health) -> String {
    let mut parts = vec![format!(
        "{} for {} min",
        h.status.as_deref().unwrap_or("unknown"),
        h.status_for_minutes.map_or("?".into(), |m| m.to_string())
    )];
    parts.push(match (&h.latest_failure, h.failures) {
        (_, 0) => "no failures".into(),
        (Some(cause), n) => format!("{n} failures ({cause})"),
        (None, n) => format!("{n} failures"),
    });
    parts.push(format!("{} checkpoints completed", h.checkpoints_completed));
    if let Some(lag) = h.lag_records {
        let trend = h.lag_change_per_minute.map(|c| format!(", {c:+}/min")).unwrap_or_default();
        parts.push(format!("lag {lag} records{trend}"));
    }
    if let Some(bp) = h.backpressured_ms_per_second {
        parts.push(format!("backpressure {bp} ms/s"));
    }
    format!("{} (last {} min)", parts.join(", "), h.window_minutes)
}

pub fn acted(plan: &Plan, detail: &str, verify_in_seconds: u64) -> String {
    format!("Done: {}{detail}. Verifying in {verify_in_seconds} s.", plan.describe())
}

pub fn verified(recovered: f64, after: &Health) -> String {
    format!("Verification: recovered {} — {}.", percent(recovered), health(after))
}

/// The ticket's close notes once the job has recovered.
pub fn resolved(plan: &Plan, owner: &str, before: &Health, after: &Health) -> String {
    match plan {
        Plan::Rollback { from_version, to_version } => format!(
            "Rolled back {from_version} → {to_version} automatically; the job is {}. Follow-up for {owner}: fix \
             {from_version} before deploying it again.",
            health(after)
        ),
        Plan::ScaleUp { from, to } => format!(
            "Scaled parallelism {from} → {to} automatically. Lag {} → {} records. Parallelism stays at {to} until \
             the next deploy; {owner} may want to keep it.",
            before.lag_records.map_or("?".into(), |l| l.to_string()),
            after.lag_records.map_or("?".into(), |l| l.to_string()),
        ),
        Plan::Restart { version } => {
            format!("Restarted {version} from its last checkpoint automatically; the job is {}.", health(after))
        }
    }
}

/// What the owning team needs to do, by failure class.
pub fn fix_hint(class: Option<FailureClass>, situation_state: &Value) -> String {
    let growth = situation_state["evidence"]["checkpoints"]["state_growth_mb_per_minute"].as_f64();
    match class {
        Some(FailureClass::StateGrowth) => format!(
            "State keeps growing{}; it needs a code change, such as a state TTL. Restarting only buys time.",
            growth.map(|g| format!(" ({g:+} MB/min)")).unwrap_or_default()
        ),
        Some(FailureClass::BadInputData) => {
            "Records the job can't parse keep failing it; the producer, or the job's handling of bad records \
             (e.g. a dead-letter queue), needs a change."
                .into()
        }
        Some(FailureClass::SchemaMismatch) => {
            "The input's schema no longer matches the job; the producer and the owning team need to agree a fix.".into()
        }
        Some(FailureClass::CodeRegression) => "The job's code fails on its input; it needs a code fix.".into(),
        Some(FailureClass::PlatformInfrastructure) => {
            "The platform beneath the job is at fault; the platform team needs to look.".into()
        }
        _ => "The evidence doesn't point to a fix the operator may make.".into(),
    }
}

pub fn escalated(team: &str, why: &str, hint: &str) -> String {
    format!("Escalated to {team}: {why}. {hint}")
}
