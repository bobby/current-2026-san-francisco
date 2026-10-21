//! The tickets mock (docs/design.md §9.2): an incident table shaped like the
//! ServiceNow Table API (`/api/now/table/incident`). Field names and
//! string-typed values follow ServiceNow, so a real ServiceNow client works
//! against it and these types describe what the service serves.

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tickets request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("tickets service rejected the request ({status}): {body}")]
    Rejected { status: u16, body: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// One incident record, as the Table API returns it. Every value is a
/// string, as in ServiceNow; times are `YYYY-MM-DD HH:MM:SS` in UTC, and
/// empty when unset.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Incident {
    pub sys_id: String,
    /// `INC0010001`, `INC0010002`, …
    pub number: String,
    pub short_description: String,
    pub description: String,
    /// See [`IncidentState`].
    pub state: String,
    /// `1` (critical) to `5` (planning).
    pub priority: String,
    pub impact: String,
    pub urgency: String,
    pub category: String,
    pub assignment_group: String,
    pub assigned_to: String,
    pub caller_id: String,
    /// The configuration item: the platform job name.
    pub cmdb_ci: String,
    /// The agents' incident ID, linking the ticket to `platform.signals` and
    /// `platform.actions`.
    pub correlation_id: String,
    pub close_code: String,
    pub close_notes: String,
    pub opened_at: String,
    pub resolved_at: String,
    pub closed_at: String,
    pub sys_created_on: String,
    pub sys_created_by: String,
    pub sys_updated_on: String,
    pub sys_updated_by: String,
    pub sys_mod_count: String,
    /// The work-notes journal, newest first, as ServiceNow displays it.
    pub work_notes: String,
    /// The customer-visible comments journal, newest first.
    pub comments: String,
}

impl Incident {
    pub fn state(&self) -> Option<IncidentState> {
        IncidentState::from_code(&self.state)
    }
}

/// ServiceNow's incident states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncidentState {
    New,
    InProgress,
    OnHold,
    Resolved,
    Closed,
    Canceled,
}

impl IncidentState {
    pub const ALL: [Self; 6] =
        [Self::New, Self::InProgress, Self::OnHold, Self::Resolved, Self::Closed, Self::Canceled];

    pub fn code(self) -> &'static str {
        match self {
            Self::New => "1",
            Self::InProgress => "2",
            Self::OnHold => "3",
            Self::Resolved => "6",
            Self::Closed => "7",
            Self::Canceled => "8",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::New => "New",
            Self::InProgress => "In Progress",
            Self::OnHold => "On Hold",
            Self::Resolved => "Resolved",
            Self::Closed => "Closed",
            Self::Canceled => "Canceled",
        }
    }

    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|s| s.code() == code)
    }

    /// Still being worked: not resolved, closed or canceled.
    pub fn is_active(self) -> bool {
        matches!(self, Self::New | Self::InProgress | Self::OnHold)
    }
}

/// The fields to set when creating or updating an incident. Unset fields are
/// left alone. `work_notes` and `comments` add a journal entry.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IncidentFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub short_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impact: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub urgency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignment_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assigned_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caller_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmdb_ci: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work_notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comments: Option<String>,
}

/// What the tickets service tells the chatops-bot after each change, so the
/// ticket's card in Mattermost stays current.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TicketNotification {
    pub event: TicketEvent,
    pub incident: Incident,
    /// The fields the change set.
    pub changed: Vec<String>,
    /// The journal entry the change added, if any.
    pub note: Option<JournalEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TicketEvent {
    Created,
    Updated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalEntry {
    /// `work_notes` or `comments`.
    pub element: String,
    pub value: String,
    pub created_by: String,
    pub created_on: String,
}

#[derive(Deserialize)]
struct One {
    result: Incident,
}

#[derive(Deserialize)]
struct Many {
    result: Vec<Incident>,
}

/// A Table API client. Requests authenticate as `user`, which the mock
/// records as the author of each change (it doesn't check passwords).
#[derive(Debug, Clone)]
pub struct TicketsClient {
    base_url: String,
    user: String,
    http: reqwest::Client,
}

impl TicketsClient {
    pub fn new(base_url: &str, user: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            user: user.to_string(),
            http: reqwest::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/now/table/incident{path}", self.base_url)
    }

    async fn send<T: serde::de::DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let response = request.basic_auth(&self.user, Some("")).header("Accept", "application/json").send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Rejected { status: status.as_u16(), body });
        }
        Ok(response.json().await?)
    }

    pub async fn create(&self, fields: &IncidentFields) -> Result<Incident> {
        Ok(self.send::<One>(self.http.post(self.url("")).json(fields)).await?.result)
    }

    pub async fn get(&self, sys_id: &str) -> Result<Incident> {
        Ok(self.send::<One>(self.http.get(self.url(&format!("/{sys_id}")))).await?.result)
    }

    pub async fn update(&self, sys_id: &str, fields: &IncidentFields) -> Result<Incident> {
        Ok(self.send::<One>(self.http.patch(self.url(&format!("/{sys_id}"))).json(fields)).await?.result)
    }

    /// Incidents matching an encoded query, e.g. `cmdb_ci=payments-enrichment^active=true^ORDERBYDESCsys_created_on`.
    pub async fn query(&self, sysparm_query: &str, limit: u32) -> Result<Vec<Incident>> {
        let request = self
            .http
            .get(self.url(""))
            .query(&[("sysparm_query", sysparm_query), ("sysparm_limit", &limit.to_string())]);
        Ok(self.send::<Many>(request).await?.result)
    }

    pub async fn find_by_number(&self, number: &str) -> Result<Option<Incident>> {
        Ok(self.query(&format!("number={number}"), 1).await?.into_iter().next())
    }
}
