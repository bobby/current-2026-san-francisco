//! Card templates: each `Card` becomes a Mattermost message attachment
//! (colour bar, title link, fields, buttons). All Mattermost formatting lives
//! here; agents send typed cards.

use chrono::{DateTime, Utc};
use platform_clients::{
    chatops::{ApprovalButton, ApprovalCard, ButtonStyle, Card, Field, IncidentCard, IncidentStatus, SignalCard},
    tickets::{Incident, IncidentState},
};
use platform_model::{ActionKind, ActionPhase, AlertEvent, AlertStatus, Disposition, FailureClass};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const RED: &str = "#d24b4b";
const AMBER: &str = "#e3a21a";
const GREEN: &str = "#3d9970";
const BLUE: &str = "#2f6fdb";
const PURPLE: &str = "#7b5cc4";
const GREY: &str = "#8a8f98";

/// Where links and button callbacks point.
#[derive(Debug, Clone)]
pub struct Links {
    /// Grafana as people reach it, for evidence links.
    pub grafana: String,
    /// The tickets UI as people reach it.
    pub tickets: String,
    /// The bot as Mattermost reaches it, for button callbacks.
    pub callback: String,
}

/// A rendered post: the plain message and the attachment.
#[derive(Debug, Clone, PartialEq)]
pub struct Rendered {
    pub message: String,
    pub attachment: Value,
}

impl Rendered {
    pub fn props(&self) -> Value {
        json!({"attachments": [self.attachment]})
    }
}

/// What a button click sends back to the bot (the action's integration
/// context). Mattermost keeps it server-side; clients never see it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ButtonContext {
    pub action_id: Uuid,
    pub job_name: String,
    pub incident_id: Option<Uuid>,
    pub signal_id: Option<Uuid>,
    pub button: ApprovalButton,
}

pub fn render(card: &Card, links: &Links) -> Rendered {
    match card {
        Card::Alert(alert) => alert_card(alert),
        Card::Signal(signal) => signal_card(signal, links),
        Card::Incident(incident) => incident_card(incident, links),
        Card::Ticket(ticket) => ticket_card(ticket, links),
        Card::Approval(approval) => approval_card(approval, links),
    }
}

fn fields(fields: &[Field]) -> Vec<Value> {
    fields.iter().map(|f| json!({"title": f.title, "value": f.value, "short": f.short})).collect()
}

fn short_id(id: &Uuid) -> String {
    id.simple().to_string()[..8].to_string()
}

fn time(ts: &DateTime<Utc>) -> String {
    ts.format("%H:%M:%S UTC").to_string()
}

fn percent(p: Option<f64>) -> String {
    p.map_or("—".into(), |p| format!("{:.0}%", p * 100.0))
}

/// Prometheus rewrites job names in labels (`payments-enrichment` →
/// `payments_enrichment`); the dashboard's job variable uses the label form.
fn label_value(value: &str) -> String {
    value.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' }).collect()
}

/// The platform dashboard for one job, from 30 minutes before `at` to 15
/// after.
pub fn dashboard_link(links: &Links, job_name: &str, at: &DateTime<Utc>) -> String {
    let from = at.timestamp_millis() - 30 * 60 * 1000;
    let to = at.timestamp_millis() + 15 * 60 * 1000;
    format!(
        "{}/d/platform-overview/platform-overview?var-job={}&from={from}&to={to}",
        links.grafana,
        label_value(job_name)
    )
}

pub fn ticket_link(links: &Links, number: &str) -> String {
    format!("{}/incident/{number}", links.tickets)
}

fn alert_card(alert: &AlertEvent) -> Rendered {
    let firing = alert.status == AlertStatus::Firing;
    let color = match (firing, alert.severity.as_deref()) {
        (false, _) => GREEN,
        (true, Some("critical")) => RED,
        (true, _) => AMBER,
    };
    let status = if firing { "FIRING" } else { "RESOLVED" };
    let text =
        alert.annotations.get("summary").or_else(|| alert.annotations.get("description")).cloned().unwrap_or_default();
    let mut f = vec![
        Field::short("Job", alert.job_name.as_deref().unwrap_or("—")),
        Field::short("Tenant", alert.tenant.as_deref().unwrap_or("—")),
        Field::short("Severity", alert.severity.as_deref().unwrap_or("—")),
        Field::short(if firing { "Since" } else { "Resolved" }, time(&alert.ts)),
    ];
    if f[0].value == "—" {
        f.remove(0);
    }
    Rendered {
        message: String::new(),
        attachment: json!({
            "fallback": format!("{status}: {} {}", alert.alertname, alert.job_name.as_deref().unwrap_or("")),
            "color": color,
            "title": format!("{status}: {}", alert.alertname),
            "text": text,
            "fields": fields(&f),
            "footer": format!("Alertmanager · {}", alert.fingerprint),
        }),
    }
}

pub fn failure_class_label(class: FailureClass) -> &'static str {
    match class {
        FailureClass::CodeRegression => "code regression",
        FailureClass::BadInputData => "bad input data",
        FailureClass::ResourceSaturation => "resource saturation",
        FailureClass::StateGrowth => "state growth",
        FailureClass::SchemaMismatch => "schema mismatch",
        FailureClass::PlatformInfrastructure => "platform infrastructure",
        FailureClass::TransientOrExpected => "transient or expected",
        FailureClass::Unknown => "unknown cause",
    }
}

/// The user-impact score's scale (docs/design.md §8.1).
pub fn impact_label(score: f64) -> &'static str {
    match score {
        s if s < 1.0 / 6.0 => "none",
        s if s < 0.5 => "slight delay",
        s if s < 5.0 / 6.0 => "stale data",
        _ => "stopped / data at risk",
    }
}

fn signal_card(card: &SignalCard, links: &Links) -> Rendered {
    let s = &card.signal;
    let impact = s.user_impact.unwrap_or(0.0);
    let color = match s.disposition {
        Disposition::Raise if impact >= 0.5 => RED,
        Disposition::Raise => AMBER,
        Disposition::Attach => BLUE,
        Disposition::Suppress => GREY,
    };
    let class = s.failure_class.map_or("unclassified", failure_class_label);
    let disposition = match s.disposition {
        Disposition::Raise => "raised",
        Disposition::Attach => "attached to an open incident",
        Disposition::Suppress => "suppressed",
    };
    let mut f = vec![
        Field::short("Tenant", &s.tenant),
        Field::short("Disposition", disposition),
        Field::short("Actionable", percent(s.actionable)),
        Field::short("User impact", s.user_impact.map_or("—".to_string(), |i| format!("{} ({i:.2})", impact_label(i)))),
        Field::short("Deploy-related", percent(s.deploy_related)),
        Field::short("Alerts", s.alert_event_ids.len().to_string()),
    ];
    if let Some(d) = &card.deployment {
        let age = (s.ts - d.ts).num_minutes();
        f.push(Field::long(
            "Latest deploy",
            format!(
                "{} → {} by {}, {age} min before: {}",
                d.from_version.as_deref().unwrap_or("∅"),
                d.to_version,
                d.author,
                d.summary
            ),
        ));
    }
    let text = s.judgment.get("summary").and_then(Value::as_str).unwrap_or_default();
    let incident = s.incident_id.map(|id| format!(" · incident {}", short_id(&id))).unwrap_or_default();
    Rendered {
        message: String::new(),
        attachment: json!({
            "fallback": format!("{}: {class} ({disposition})", s.job_name),
            "color": color,
            "title": format!("{}: {class}", s.job_name),
            "title_link": dashboard_link(links, &s.job_name, &s.ts),
            "text": text,
            "fields": fields(&f),
            "footer": format!("triage-bot · signal {}{incident}", short_id(&s.signal_id)),
        }),
    }
}

fn incident_card(card: &IncidentCard, links: &Links) -> Rendered {
    let (color, status) = match card.status {
        IncidentStatus::Investigating => (AMBER, "🔎 Investigating"),
        IncidentStatus::Remediating => (BLUE, "🛠️ Remediating"),
        IncidentStatus::Verifying => (PURPLE, "⏱️ Verifying"),
        IncidentStatus::Resolved => (GREEN, "✅ Resolved"),
        IncidentStatus::Escalated => (RED, "🙋 Escalated"),
    };
    let mut f =
        vec![Field::short("Job", &card.job_name), Field::short("Tenant", &card.tenant), Field::short("Status", status)];
    if let Some(ticket) = &card.ticket {
        f.push(Field::short("Ticket", format!("[{ticket}]({})", ticket_link(links, ticket))));
    }
    f.extend(card.fields.iter().cloned());
    let message = card.owner.as_ref().map(|owner| format!("FYI {owner}")).unwrap_or_default();
    Rendered {
        message,
        attachment: json!({
            "fallback": format!("{status}: {}", card.title),
            "color": color,
            "title": card.title,
            "title_link": dashboard_link(links, &card.job_name, &Utc::now()),
            "text": card.summary,
            "fields": fields(&f),
            "footer": format!("operator-bot · incident {}", short_id(&card.incident_id)),
        }),
    }
}

/// The newest entry of a ServiceNow journal (newest first, entries separated
/// by a blank line), without its header line.
fn latest_note(journal: &str) -> Option<String> {
    let entry = journal.split("\n\n").next()?.trim();
    let body = entry.split_once('\n').map_or(entry, |(_, body)| body).trim();
    (!body.is_empty()).then(|| body.to_string())
}

fn ticket_card(ticket: &Incident, links: &Links) -> Rendered {
    let state = ticket.state();
    let color = match state {
        Some(IncidentState::New) => BLUE,
        Some(IncidentState::InProgress) => AMBER,
        Some(IncidentState::Resolved) => GREEN,
        Some(IncidentState::OnHold | IncidentState::Closed | IncidentState::Canceled) | None => GREY,
    };
    let or_dash = |s: &str| if s.is_empty() { "—".to_string() } else { s.to_string() };
    let mut f = vec![
        Field::short("State", state.map_or(ticket.state.clone(), |s| s.label().to_string())),
        Field::short("Priority", or_dash(&ticket.priority)),
        Field::short("Assignment group", or_dash(&ticket.assignment_group)),
        Field::short("Assigned to", or_dash(&ticket.assigned_to)),
        Field::short("Job", or_dash(&ticket.cmdb_ci)),
    ];
    if let Some(note) = latest_note(&ticket.work_notes) {
        f.push(Field::long("Latest work note", note));
    }
    if !ticket.close_notes.is_empty() {
        f.push(Field::long("Close notes", &ticket.close_notes));
    }
    Rendered {
        message: String::new(),
        attachment: json!({
            "fallback": format!("{}: {}", ticket.number, ticket.short_description),
            "color": color,
            "title": format!("{}: {}", ticket.number, ticket.short_description),
            "title_link": ticket_link(links, &ticket.number),
            "fields": fields(&f),
            "footer": format!("Tickets · updated {} UTC by {}", ticket.sys_updated_on, ticket.sys_updated_by),
        }),
    }
}

pub fn action_label(action: ActionKind) -> &'static str {
    match action {
        ActionKind::RollbackToPreviousVersion => "Roll back to the previous version",
        ActionKind::RestartFromCheckpoint => "Restart from the last checkpoint",
        ActionKind::ScaleUp => "Scale up",
        ActionKind::Redeploy => "Redeploy",
        ActionKind::OpenTicket => "Open a ticket",
        ActionKind::UpdateTicket => "Update the ticket",
        ActionKind::EscalateToOwner => "Escalate to the owning team",
        ActionKind::EscalateToPlatform => "Escalate to the platform team",
        ActionKind::Observe => "Keep watching",
    }
}

/// Parameters as `key: value` lines.
fn params_text(params: &Value) -> String {
    match params {
        Value::Object(map) if !map.is_empty() => map
            .iter()
            .map(|(k, v)| format!("`{k}`: {}", v.as_str().map_or_else(|| v.to_string(), str::to_string)))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn approval_card(card: &ApprovalCard, links: &Links) -> Rendered {
    let mut f = vec![Field::short("Job", &card.job_name), Field::short("Proposed by", &card.proposed_by)];
    if let Some(first) = card.buttons.first() {
        let params = params_text(&first.params);
        if !params.is_empty() {
            f.push(Field::long("Parameters", params));
        }
    }
    f.extend(card.fields.iter().cloned());
    let actions: Vec<Value> = card
        .buttons
        .iter()
        .enumerate()
        .map(|(i, button)| {
            let context = ButtonContext {
                action_id: card.action_id,
                job_name: card.job_name.clone(),
                incident_id: card.incident_id,
                signal_id: card.signal_id,
                button: button.clone(),
            };
            json!({
                // Mattermost wants letters and digits only.
                "id": format!("button{i}"),
                "name": button.label,
                "type": "button",
                "style": match button.style {
                    ButtonStyle::Primary => "primary",
                    ButtonStyle::Danger => "danger",
                    ButtonStyle::Default => "default",
                },
                "integration": {"url": format!("{}/actions", links.callback), "context": context},
            })
        })
        .collect();
    Rendered {
        message: String::new(),
        attachment: json!({
            "fallback": format!("Approval needed: {}", card.title),
            "color": BLUE,
            "pretext": "**Approval needed**",
            "title": card.title,
            "title_link": dashboard_link(links, &card.job_name, &Utc::now()),
            "text": card.rationale,
            "fields": fields(&f),
            "actions": actions,
            "footer": format!("action {}", short_id(&card.action_id)),
        }),
    }
}

/// The approval card after a decision: the buttons give way to who decided
/// what.
pub fn decided(mut attachment: Value, button: &ApprovalButton, user: &str) -> Value {
    let (icon, verb, color) = match button.phase {
        ActionPhase::Approved => ("✅", "Approved", GREEN),
        ActionPhase::Rejected => ("❌", "Rejected", GREY),
        _ => ("☑️", "Decided", GREY),
    };
    if let Some(obj) = attachment.as_object_mut() {
        obj.remove("actions");
        obj.insert("color".into(), json!(color));
        obj.insert("pretext".into(), json!(format!("**{icon} {verb} by @{user}**")));
        let decision = json!({
            "title": "Decision",
            "value": format!("{}: {} by @{user} at {}", action_label(button.action), verb.to_lowercase(), time(&Utc::now())),
            "short": false,
        });
        match obj.get_mut("fields").and_then(Value::as_array_mut) {
            Some(fields) => fields.push(decision),
            None => {
                obj.insert("fields".into(), json!([decision]));
            }
        }
    }
    attachment
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_model::Signal;

    fn links() -> Links {
        Links {
            grafana: "http://localhost:3000".into(),
            tickets: "http://localhost:8094".into(),
            callback: "http://chatops-bot:8092".into(),
        }
    }

    #[test]
    fn ticket_card_links_to_its_page_and_shows_the_latest_note() {
        let ticket = Incident {
            number: "INC0010001".into(),
            short_description: "payments-enrichment restart loop".into(),
            state: "2".into(),
            priority: "2".into(),
            cmdb_ci: "payments-enrichment".into(),
            work_notes: "2026-10-21 10:05:00 - operator-bot (Work notes)\nRolled back to 1.41.0\n\n\
                         2026-10-21 10:01:00 - operator-bot (Work notes)\nOpened"
                .into(),
            ..Default::default()
        };
        let a = render(&Card::Ticket(ticket), &links()).attachment;
        assert_eq!(a["title_link"], "http://localhost:8094/incident/INC0010001");
        assert_eq!(a["color"], AMBER);
        let note = a["fields"].as_array().unwrap().iter().find(|f| f["title"] == "Latest work note").unwrap();
        assert_eq!(note["value"], "Rolled back to 1.41.0");
    }

    #[test]
    fn signal_card_links_to_the_dashboard_with_the_label_form_of_the_job() {
        let ts: DateTime<Utc> = "2026-10-21T10:00:00Z".parse().unwrap();
        let signal = Signal {
            signal_id: Uuid::nil(),
            ts,
            job_name: "payments-enrichment".into(),
            tenant: "payments-eng".into(),
            alert_event_ids: vec![Uuid::nil(), Uuid::nil()],
            disposition: Disposition::Raise,
            incident_id: None,
            failure_class: Some(FailureClass::CodeRegression),
            actionable: Some(0.93),
            user_impact: Some(0.9),
            deploy_related: Some(0.88),
            deployment_event_id: None,
            judgment: json!({}),
        };
        let a = render(&Card::Signal(SignalCard { signal, deployment: None }), &links()).attachment;
        assert_eq!(a["title"], "payments-enrichment: code regression");
        assert_eq!(a["color"], RED);
        assert!(a["title_link"].as_str().unwrap().contains("var-job=payments_enrichment&from="));
        assert!(a["fields"].to_string().contains("stopped / data at risk (0.90)"));
    }

    #[test]
    fn approval_buttons_carry_their_decision_and_the_decision_replaces_them() {
        let card = ApprovalCard {
            action_id: Uuid::nil(),
            job_name: "trade-events-ingest".into(),
            incident_id: None,
            signal_id: None,
            title: "Redeploy trade-events-ingest with a dead-letter queue".into(),
            rationale: "Malformed records from one producer".into(),
            proposed_by: "ops-assistant".into(),
            fields: vec![],
            buttons: ApprovalCard::approve_or_reject(ActionKind::Redeploy, json!({"args": "--on-deser-error=dlq"})),
        };
        let a = render(&Card::Approval(card), &links()).attachment;
        let actions = a["actions"].as_array().unwrap();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0]["integration"]["url"], "http://chatops-bot:8092/actions");
        let context: ButtonContext = serde_json::from_value(actions[1]["integration"]["context"].clone()).unwrap();
        assert_eq!(context.button.phase, ActionPhase::Rejected);

        let after = decided(a, &context.button, "oncall");
        assert!(after.get("actions").is_none());
        assert!(after["pretext"].as_str().unwrap().contains("Rejected by @oncall"));
    }

    #[test]
    fn impact_scale() {
        assert_eq!(impact_label(0.0), "none");
        assert_eq!(impact_label(0.3), "slight delay");
        assert_eq!(impact_label(0.6), "stale data");
        assert_eq!(impact_label(1.0), "stopped / data at risk");
    }
}
