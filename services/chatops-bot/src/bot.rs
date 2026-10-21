//! The bot's state and behaviour: posting cards, keeping ticket cards
//! current, recording button decisions and answering ticket lookups.

use std::collections::{HashMap, HashSet};

use anyhow::Context;
use chrono::Utc;
use platform_clients::{
    chatops::{Bot, Card, Channel, PostRequest, PostResponse},
    kafka::Publisher,
    tickets::{TicketNotification, TicketsClient},
};
use platform_model::{ActionEvent, ActionPhase, topics};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    cards::{self, ButtonContext, Links, Rendered},
    mattermost::{Mattermost, Post},
    setup::{self, Admin, Workspace},
};

/// Where a ticket's card is: the post, and the thread it belongs to.
#[derive(Debug, Clone)]
struct TicketPost {
    post_id: String,
    root_id: String,
}

pub struct Chatops {
    pub mm: Mattermost,
    pub workspace: RwLock<Workspace>,
    admin: Admin,
    links: Links,
    publisher: Publisher,
    tickets: TicketsClient,
    /// Ticket `sys_id` → its card.
    ticket_posts: Mutex<HashMap<String, TicketPost>>,
    /// Incident ID → the post that roots its thread, so the incident's
    /// ticket card goes into that thread.
    incident_threads: Mutex<HashMap<String, String>>,
    /// Actions already decided by a button click.
    decided: Mutex<HashSet<Uuid>>,
}

/// A button click, as Mattermost sends it.
#[derive(Debug, Deserialize)]
pub struct ActionRequest {
    pub user_id: String,
    #[serde(default)]
    pub user_name: String,
    pub post_id: String,
    pub context: ButtonContext,
}

impl Chatops {
    pub fn new(
        mm: Mattermost,
        workspace: Workspace,
        admin: Admin,
        links: Links,
        publisher: Publisher,
        tickets: TicketsClient,
    ) -> Self {
        Self {
            mm,
            workspace: RwLock::new(workspace),
            admin,
            links,
            publisher,
            tickets,
            ticket_posts: Default::default(),
            incident_threads: Default::default(),
            decided: Default::default(),
        }
    }

    fn content(&self, request: &PostRequest) -> anyhow::Result<(Bot, Rendered)> {
        let rendered = match &request.card {
            Some(card) => cards::render(card, &self.links),
            None => Rendered { message: String::new(), attachment: Value::Null },
        };
        let message = match (&request.text, rendered.message.is_empty()) {
            (Some(text), true) => text.clone(),
            (Some(text), false) => format!("{text}\n{}", rendered.message),
            (None, _) => rendered.message.clone(),
        };
        anyhow::ensure!(!message.is_empty() || request.card.is_some(), "a post needs text or a card");
        let bot = request.bot.or(request.card.as_ref().map(Card::default_bot)).unwrap_or(Bot::OperatorBot);
        Ok((bot, Rendered { message, ..rendered }))
    }

    fn props(rendered: &Rendered) -> Value {
        if rendered.attachment.is_null() { json!({}) } else { rendered.props() }
    }

    pub async fn post(&self, request: &PostRequest) -> anyhow::Result<PostResponse> {
        let (bot, rendered) = self.content(request)?;
        let (token, channel_id) = {
            let ws = self.workspace.read().await;
            (ws.bot_token(bot).to_string(), ws.channel_id(request.channel).to_string())
        };
        let post = Post {
            channel_id: channel_id.clone(),
            root_id: request.root_id.clone().unwrap_or_default(),
            message: rendered.message.clone(),
            props: Self::props(&rendered),
            ..Default::default()
        };
        let created = self.mm.create_post(&token, &post).await?;
        if let (Some(Card::Incident(incident)), None) = (&request.card, &request.root_id) {
            self.incident_threads.lock().await.insert(incident.incident_id.to_string(), created.id.clone());
        }
        Ok(PostResponse { post_id: created.id, channel_id })
    }

    /// Replaces a post's content. The post must be the same bot's.
    pub async fn update(&self, post_id: &str, request: &PostRequest) -> anyhow::Result<PostResponse> {
        let (bot, rendered) = self.content(request)?;
        let token = self.workspace.read().await.bot_token(bot).to_string();
        let post = self.mm.patch_post(&token, post_id, &rendered.message, &Self::props(&rendered)).await?;
        Ok(PostResponse { post_id: post.id, channel_id: post.channel_id })
    }

    /// The raw alert firehose.
    pub async fn post_alert(&self, alert: platform_model::AlertEvent) -> anyhow::Result<()> {
        let request = PostRequest {
            channel: Channel::PlatformAlerts,
            bot: None,
            root_id: None,
            text: None,
            card: Some(Card::Alert(alert)),
        };
        self.post(&request).await.map(drop)
    }

    /// Keeps a ticket's card current. A new ticket gets a card in
    /// `#platform-incidents`, inside its incident's thread if the bot knows
    /// it (by `correlation_id`). An update edits the card and adds any new
    /// note to the thread.
    pub async fn on_ticket(&self, n: TicketNotification) -> anyhow::Result<()> {
        let rendered = cards::render(&Card::Ticket(n.incident.clone()), &self.links);
        let (token, channel_id) = {
            let ws = self.workspace.read().await;
            (ws.bot_token(Bot::Tickets).to_string(), ws.channel_id(Channel::PlatformIncidents).to_string())
        };
        let known = self.ticket_posts.lock().await.get(&n.incident.sys_id).cloned();
        match known {
            Some(card) => {
                self.mm.patch_post(&token, &card.post_id, &rendered.message, &rendered.props()).await?;
                let reply = match (&n.note, n.changed.iter().any(|c| c == "state")) {
                    (Some(note), _) => {
                        let kind = if note.element == "work_notes" { "Work note" } else { "Comment" };
                        Some(format!("📝 **{}** · {kind} from {}:\n{}", n.incident.number, note.created_by, note.value))
                    }
                    (None, true) => {
                        n.incident.state().map(|s| format!("🔁 **{}** is now {}", n.incident.number, s.label()))
                    }
                    (None, false) => None,
                };
                if let Some(message) = reply {
                    let post = Post { channel_id, root_id: card.root_id, message, ..Default::default() };
                    self.mm.create_post(&token, &post).await?;
                }
            }
            None => {
                let thread = self.incident_threads.lock().await.get(&n.incident.correlation_id).cloned();
                let post = Post {
                    channel_id,
                    root_id: thread.clone().unwrap_or_default(),
                    message: rendered.message.clone(),
                    props: rendered.props(),
                    ..Default::default()
                };
                let created = self.mm.create_post(&token, &post).await?;
                let root_id = thread.unwrap_or_else(|| created.id.clone());
                self.ticket_posts
                    .lock()
                    .await
                    .insert(n.incident.sys_id.clone(), TicketPost { post_id: created.id, root_id });
                info!(number = n.incident.number, "ticket card posted");
            }
        }
        Ok(())
    }

    /// Records a button decision in `platform.actions` and replaces the
    /// buttons with who decided what. Returns Mattermost's response body.
    pub async fn on_action(&self, request: ActionRequest) -> anyhow::Result<Value> {
        let ctx = &request.context;
        if !self.decided.lock().await.insert(ctx.action_id) {
            return Ok(json!({"ephemeral_text": "Someone already decided on this."}));
        }
        let admin_token = self.workspace.read().await.admin_token.clone();
        let user = if request.user_name.is_empty() {
            self.mm.user(&admin_token, &request.user_id).await?.username
        } else {
            request.user_name.clone()
        };
        let phase = ctx.button.phase;
        let event = ActionEvent {
            event_id: platform_model::event_id(&["chatops-decision", &ctx.action_id.to_string(), phase_name(phase)]),
            ts: Utc::now(),
            action_id: ctx.action_id,
            job_name: ctx.job_name.clone(),
            incident_id: ctx.incident_id,
            signal_id: ctx.signal_id,
            actor: format!("user:{user}"),
            action: ctx.button.action,
            phase,
            params: ctx.button.params.clone(),
            rationale: None,
            ticket: None,
        };
        if let Err(err) = self.publisher.publish(topics::ACTIONS, &event.job_name, &event).await {
            self.decided.lock().await.remove(&ctx.action_id);
            return Err(err).context("recording the decision");
        }
        info!(action_id = %ctx.action_id, job = ctx.job_name, phase = phase_name(phase), user, "decision recorded");

        let post = self.mm.get_post(&admin_token, &request.post_id).await?;
        let attachment = post.props.get("attachments").and_then(|a| a.get(0)).cloned().unwrap_or_else(|| json!({}));
        Ok(json!({
            "update": {"message": post.message, "props": {"attachments": [cards::decided(attachment, &ctx.button, &user)]}},
            "ephemeral_text": format!("Recorded: {} {}", ctx.button.label.to_lowercase(), cards::action_label(ctx.button.action).to_lowercase()),
        }))
    }

    /// Answers a human's post that mentions ticket numbers with each ticket's
    /// card, in the post's thread.
    pub async fn on_posted(&self, post: Post) {
        if post.user_id.is_empty() || self.workspace.read().await.is_bot(&post.user_id) {
            return;
        }
        let numbers = ticket_numbers(&post.message);
        if numbers.is_empty() {
            return;
        }
        let token = self.workspace.read().await.bot_token(Bot::Tickets).to_string();
        let root_id = if post.root_id.is_empty() { post.id.clone() } else { post.root_id.clone() };
        for number in numbers {
            let ticket = match self.tickets.find_by_number(&number).await {
                Ok(Some(ticket)) => ticket,
                Ok(None) => continue,
                Err(err) => {
                    warn!(%err, number, "looking up a ticket");
                    continue;
                }
            };
            let rendered = cards::render(&Card::Ticket(ticket), &self.links);
            let reply = Post {
                channel_id: post.channel_id.clone(),
                root_id: root_id.clone(),
                message: rendered.message.clone(),
                props: rendered.props(),
                ..Default::default()
            };
            if let Err(err) = self.mm.create_post(&token, &reply).await {
                warn!(%err, number, "answering a ticket lookup");
            }
        }
    }

    /// Archives and re-creates the channels, and forgets the old threads.
    pub async fn reset(&self) -> anyhow::Result<()> {
        let mut ws = self.workspace.write().await;
        setup::reset_channels(&self.mm, &mut ws).await?;
        self.ticket_posts.lock().await.clear();
        self.incident_threads.lock().await.clear();
        self.decided.lock().await.clear();
        Ok(())
    }

    /// Logs the admin in again, e.g. after the session expired.
    pub async fn relogin(&self) -> anyhow::Result<()> {
        let token = self.mm.login(&self.admin.username, &self.admin.password).await?;
        self.workspace.write().await.admin_token = token;
        Ok(())
    }
}

fn phase_name(phase: ActionPhase) -> &'static str {
    match phase {
        ActionPhase::Proposed => "proposed",
        ActionPhase::Approved => "approved",
        ActionPhase::Rejected => "rejected",
        ActionPhase::Executed => "executed",
        ActionPhase::Failed => "failed",
        ActionPhase::Verified => "verified",
    }
}

/// Ticket numbers (`INC` and seven digits) in a message, each once.
pub fn ticket_numbers(message: &str) -> Vec<String> {
    let bytes = message.as_bytes();
    let mut found = Vec::new();
    let mut i = 0;
    while let Some(at) = message[i..].find("INC").map(|p| p + i) {
        let digits = &bytes[at + 3..];
        let boundary_before = at == 0 || !bytes[at - 1].is_ascii_alphanumeric();
        let seven = digits.len() >= 7 && digits[..7].iter().all(u8::is_ascii_digit);
        let boundary_after = digits.len() == 7 || (seven && !digits[7].is_ascii_alphanumeric());
        if boundary_before && seven && boundary_after {
            let number = message[at..at + 10].to_string();
            if !found.contains(&number) {
                found.push(number);
            }
        }
        i = at + 3;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_ticket_numbers() {
        assert_eq!(ticket_numbers("what's INC0010001 about? and (INC0010002)."), ["INC0010001", "INC0010002"]);
        assert_eq!(ticket_numbers("INC0010001 INC0010001"), ["INC0010001"]);
        assert!(ticket_numbers("XINC0010001 INC001 INC00100011").is_empty());
    }
}
