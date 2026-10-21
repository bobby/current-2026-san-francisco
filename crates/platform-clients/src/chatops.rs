//! The chatops-bot's API (docs/design.md §9.1). Agents and services post to
//! Mattermost through the bot, as typed cards, so every message format lives
//! in one place (`services/chatops-bot`).

use platform_model::{ActionKind, ActionPhase, AlertEvent, Deployment, Signal};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tickets::Incident;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("chatops-bot request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("chatops-bot rejected the request ({status}): {body}")]
    Rejected { status: u16, body: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// The platform's Mattermost channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Channel {
    /// The raw alert firehose.
    PlatformAlerts,
    /// Triage output (Scenario 1).
    PlatformSignals,
    /// Operator activity and tickets, one thread per incident (Scenario 3).
    PlatformIncidents,
}

impl Channel {
    pub const ALL: [Self; 3] = [Self::PlatformAlerts, Self::PlatformSignals, Self::PlatformIncidents];

    pub fn name(self) -> &'static str {
        match self {
            Self::PlatformAlerts => "platform-alerts",
            Self::PlatformSignals => "platform-signals",
            Self::PlatformIncidents => "platform-incidents",
        }
    }

    pub fn purpose(self) -> &'static str {
        match self {
            Self::PlatformAlerts => "Every alert the platform's rules fire, unfiltered",
            Self::PlatformSignals => "Alerts the triage agent judged worth a look",
            Self::PlatformIncidents => "Incidents, remediations and tickets, one thread each",
        }
    }
}

/// The bot accounts. Each stage of the maturity curve posts as its own
/// persona, so the audience can tell who is acting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Bot {
    /// Posts the raw alert firehose.
    Alertmanager,
    /// Posts ticket cards and answers ticket lookups.
    Tickets,
    /// The triage agent (Scenario 1).
    TriageBot,
    /// The operator agent (Scenario 3).
    OperatorBot,
    /// Claude Code (Scenario 2).
    OpsAssistant,
}

impl Bot {
    pub const ALL: [Self; 5] =
        [Self::Alertmanager, Self::Tickets, Self::TriageBot, Self::OperatorBot, Self::OpsAssistant];

    pub fn username(self) -> &'static str {
        match self {
            Self::Alertmanager => "alertmanager",
            Self::Tickets => "tickets",
            Self::TriageBot => "triage-bot",
            Self::OperatorBot => "operator-bot",
            Self::OpsAssistant => "ops-assistant",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Alertmanager => "Alertmanager",
            Self::Tickets => "Tickets",
            Self::TriageBot => "Triage agent",
            Self::OperatorBot => "Operator agent",
            Self::OpsAssistant => "Ops assistant (Claude)",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Alertmanager => "Relays every alert the platform's rules fire",
            Self::Tickets => "Ticket updates and lookups",
            Self::TriageBot => "Scenario 1: judges alerts and raises signals",
            Self::OperatorBot => "Scenario 3: remediates incidents within guardrails",
            Self::OpsAssistant => "Scenario 2: Claude Code, investigating on request",
        }
    }
}

/// A rich Mattermost post. The bot renders each kind from a template.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "card", rename_all = "snake_case")]
pub enum Card {
    Alert(AlertEvent),
    Signal(SignalCard),
    Incident(IncidentCard),
    Ticket(Incident),
    Approval(ApprovalCard),
}

impl Card {
    /// The persona that posts this kind of card unless the request names one.
    pub fn default_bot(&self) -> Bot {
        match self {
            Self::Alert(_) => Bot::Alertmanager,
            Self::Signal(_) => Bot::TriageBot,
            Self::Incident(_) => Bot::OperatorBot,
            Self::Ticket(_) => Bot::Tickets,
            Self::Approval(_) => Bot::OpsAssistant,
        }
    }
}

/// A triage verdict, with the deployment it considered.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalCard {
    pub signal: Signal,
    pub deployment: Option<Deployment>,
}

/// The root of an incident thread in `#platform-incidents`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentCard {
    pub incident_id: Uuid,
    pub job_name: String,
    pub tenant: String,
    /// The owning team's handle, tagged for FYI.
    pub owner: Option<String>,
    pub title: String,
    pub status: IncidentStatus,
    pub summary: String,
    #[serde(default)]
    pub fields: Vec<Field>,
    /// The ticket number, if one is open.
    pub ticket: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Investigating,
    Remediating,
    Verifying,
    Resolved,
    Escalated,
}

/// A proposed remediation that waits for a human. Each button records its
/// decision in `platform.actions` (with the clicking user as the actor).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalCard {
    pub action_id: Uuid,
    pub job_name: String,
    pub incident_id: Option<Uuid>,
    pub signal_id: Option<Uuid>,
    pub title: String,
    pub rationale: String,
    /// Who proposed it, e.g. `ops-assistant`.
    pub proposed_by: String,
    #[serde(default)]
    pub fields: Vec<Field>,
    pub buttons: Vec<ApprovalButton>,
}

impl ApprovalCard {
    /// The usual pair: Approve (records `approved`) and Reject (`rejected`).
    pub fn approve_or_reject(action: ActionKind, params: serde_json::Value) -> Vec<ApprovalButton> {
        vec![
            ApprovalButton {
                label: "Approve".into(),
                style: ButtonStyle::Primary,
                action,
                params: params.clone(),
                phase: ActionPhase::Approved,
            },
            ApprovalButton {
                label: "Reject".into(),
                style: ButtonStyle::Danger,
                action,
                params,
                phase: ActionPhase::Rejected,
            },
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalButton {
    pub label: String,
    pub style: ButtonStyle,
    /// The action this button decides on.
    pub action: ActionKind,
    pub params: serde_json::Value,
    /// The decision it records: `approved` or `rejected`.
    pub phase: ActionPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ButtonStyle {
    Default,
    Primary,
    Danger,
}

/// A labelled value on a card.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub title: String,
    pub value: String,
    /// Shown half-width, beside the next short field.
    #[serde(default)]
    pub short: bool,
}

impl Field {
    pub fn short(title: impl Into<String>, value: impl Into<String>) -> Self {
        Self { title: title.into(), value: value.into(), short: true }
    }

    pub fn long(title: impl Into<String>, value: impl Into<String>) -> Self {
        Self { title: title.into(), value: value.into(), short: false }
    }
}

/// `POST /api/posts`: post a card or text, optionally as a thread reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostRequest {
    pub channel: Channel,
    /// Defaults to the card's usual persona ([`Card::default_bot`]), or
    /// `operator-bot` for text.
    pub bot: Option<Bot>,
    /// Reply in this post's thread.
    pub root_id: Option<String>,
    pub text: Option<String>,
    pub card: Option<Card>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostResponse {
    pub post_id: String,
    pub channel_id: String,
}

#[derive(Debug, Clone)]
pub struct ChatopsClient {
    base_url: String,
    http: reqwest::Client,
}

impl ChatopsClient {
    pub fn new(base_url: &str) -> Self {
        Self { base_url: base_url.trim_end_matches('/').to_string(), http: reqwest::Client::new() }
    }

    async fn send<T: serde::de::DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Rejected { status: status.as_u16(), body });
        }
        Ok(response.json().await?)
    }

    pub async fn post(&self, request: &PostRequest) -> Result<PostResponse> {
        self.send(self.http.post(format!("{}/api/posts", self.base_url)).json(request)).await
    }

    /// Replaces a post's text and card, e.g. to show an incident's new status.
    pub async fn update(&self, post_id: &str, request: &PostRequest) -> Result<PostResponse> {
        self.send(self.http.put(format!("{}/api/posts/{post_id}", self.base_url)).json(request)).await
    }

    pub async fn card(&self, channel: Channel, root_id: Option<&str>, card: Card) -> Result<PostResponse> {
        self.post(&PostRequest {
            channel,
            bot: None,
            root_id: root_id.map(str::to_string),
            text: None,
            card: Some(card),
        })
        .await
    }

    pub async fn reply(&self, channel: Channel, root_id: &str, bot: Bot, text: &str) -> Result<PostResponse> {
        self.post(&PostRequest {
            channel,
            bot: Some(bot),
            root_id: Some(root_id.to_string()),
            text: Some(text.to_string()),
            card: None,
        })
        .await
    }
}
