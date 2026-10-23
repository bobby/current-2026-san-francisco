//! Remediations Claude proposes (Scenario 2, docs/design.md §8.2). The MCP
//! server's `propose_remediation` asks the bot to post an approval card; a
//! person's click decides. **Approve** carries the action out (through
//! cicd-sim, or Flink's REST API for a rescale) and opens the proposed
//! ticket; **Reject** is only recorded. Every step goes to
//! `platform.actions`.

use chrono::Utc;
use platform_clients::{
    chatops::{ApprovalCard, Bot, Card, Field, PostRequest, ProposalRequest, ProposalResponse},
    cicd::RequestContext,
    tickets::{IncidentFields, IncidentState},
};
use platform_model::{ActionEvent, ActionKind, ActionPhase, DeploymentOutcome, topics};
use serde_json::{Value, json};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{bot::Chatops, cards::action_label};

/// The persona that proposes, and the actor in `platform.actions`.
pub const ASSISTANT: &str = "ops-assistant";

impl Chatops {
    /// Posts an approval card for a proposal in its thread, and records it.
    pub async fn propose(&self, request: ProposalRequest) -> anyhow::Result<ProposalResponse> {
        let action_id = Uuid::new_v4();
        let mut fields = vec![Field::short("Action", action_label(request.action))];
        if let Some(ticket) = &request.ticket {
            fields.push(Field::short("Ticket for", &ticket.assignment_group));
            fields.push(Field::long("Ticket", &ticket.short_description));
        }
        let card = ApprovalCard {
            action_id,
            job_name: request.job_name.clone(),
            incident_id: request.incident_id,
            signal_id: request.signal_id,
            title: format!("{}: {}", request.job_name, action_label(request.action).to_lowercase()),
            rationale: request.rationale.clone(),
            proposed_by: ASSISTANT.into(),
            fields,
            buttons: ApprovalCard::approve_or_reject(request.action, request.params.clone()),
        };
        let posted = self
            .post(&PostRequest {
                channel: request.channel,
                bot: Some(Bot::OpsAssistant),
                root_id: Some(request.root_id.clone()),
                text: None,
                card: Some(Card::Approval(card)),
            })
            .await?;
        self.audit(
            &request,
            action_id,
            request.action,
            ActionPhase::Proposed,
            ASSISTANT,
            request.params.clone(),
            Some(request.rationale.clone()),
        )
        .await;
        info!(job = request.job_name, action = ?request.action, %action_id, "remediation proposed");
        self.proposals.lock().await.insert(action_id, request);
        Ok(ProposalResponse { action_id, post_id: posted.post_id })
    }

    /// Acts on a person's decision on a proposal; other decisions aren't
    /// the bot's to carry out.
    pub async fn carry_out(&self, action_id: Uuid, phase: ActionPhase, user: &str) {
        let Some(request) = self.proposals.lock().await.remove(&action_id) else { return };
        if phase != ActionPhase::Approved {
            self.say(&request, &format!("❌ @{user} rejected the proposal; nothing was changed.")).await;
            return;
        }
        let outcome = match self.execute(&request, action_id, user).await {
            Ok(done) => done,
            Err(err) => {
                warn!(job = request.job_name, error = format!("{err:#}"), "carrying out a remediation");
                self.audit(
                    &request,
                    action_id,
                    request.action,
                    ActionPhase::Failed,
                    "chatops-bot",
                    request.params.clone(),
                    Some(format!("{err:#}")),
                )
                .await;
                self.say(&request, &format!("⚠️ Approved by @{user}, but it failed: {err:#}")).await;
                return;
            }
        };
        let ticket = match &request.ticket {
            Some(t) => match self.open_ticket(&request, t, user).await {
                Ok(number) => format!(
                    " Opened [{number}]({}) for **{}**.",
                    crate::cards::ticket_link(&self.links, &number),
                    t.assignment_group
                ),
                Err(err) => {
                    warn!(error = format!("{err:#}"), "opening the proposed ticket");
                    format!(" Couldn't open the ticket: {err:#}")
                }
            },
            None => String::new(),
        };
        self.say(&request, &format!("✅ Approved by @{user}: {outcome}.{ticket}")).await;
    }

    /// Runs the approved action. Returns what happened, in words.
    async fn execute(&self, request: &ProposalRequest, action_id: Uuid, user: &str) -> anyhow::Result<String> {
        let job = request.job_name.as_str();
        let context = RequestContext {
            triggered_by: format!("user:{user}"),
            action_id: Some(action_id),
            incident_id: request.incident_id,
            signal_id: request.signal_id,
        };
        let params = &request.params;
        let restored = |d: &platform_model::Deployment| {
            d.restored_from.as_deref().map(|p| format!(", restored from `{}`", short_path(p))).unwrap_or_default()
        };
        match request.action {
            ActionKind::Redeploy => {
                let version = params["version"].as_str().unwrap_or_default();
                let args: Vec<String> = params["args"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                    .unwrap_or_default();
                let d = self.cicd.deploy(job, version, &args, &context).await?;
                anyhow::ensure!(d.outcome == DeploymentOutcome::Succeeded, "{}", d.failure_reason.unwrap_or_default());
                Ok(format!("redeployed {job} {version} with `{}`{}", args.join(" "), restored(&d)))
            }
            ActionKind::RollbackToPreviousVersion => {
                let d = self.cicd.rollback(job, params["to_version"].as_str(), &context).await?;
                anyhow::ensure!(d.outcome == DeploymentOutcome::Succeeded, "{}", d.failure_reason.unwrap_or_default());
                Ok(format!(
                    "rolled {job} back {} → {}{}",
                    d.from_version.as_deref().unwrap_or("?"),
                    d.to_version,
                    restored(&d)
                ))
            }
            ActionKind::ScaleUp => {
                let parallelism = params["parallelism"].as_u64().unwrap_or_default() as u32;
                let run = self.flink.active_job(job).await?.ok_or_else(|| anyhow::anyhow!("{job} isn't running"))?;
                self.flink.rescale(&run.jid, parallelism).await?;
                self.audit(
                    request,
                    action_id,
                    ActionKind::ScaleUp,
                    ActionPhase::Executed,
                    "chatops-bot",
                    params.clone(),
                    None,
                )
                .await;
                Ok(format!("rescaled {job} to parallelism {parallelism}"))
            }
            other => anyhow::bail!("{} isn't something the bot carries out", action_label(other)),
        }
    }

    async fn open_ticket(
        &self,
        request: &ProposalRequest,
        ticket: &platform_clients::chatops::TicketProposal,
        user: &str,
    ) -> anyhow::Result<String> {
        let correlation = request.incident_id.or(request.signal_id).map(|id| id.to_string());
        let fields = IncidentFields {
            short_description: Some(ticket.short_description.clone()),
            description: Some(format!(
                "{}\n\nProposed by ops-assistant (Claude) and approved by @{user}: {}.",
                ticket.description,
                action_label(request.action).to_lowercase()
            )),
            state: Some(IncidentState::New.code().into()),
            priority: Some("3".into()),
            category: Some("streaming".into()),
            assignment_group: Some(ticket.assignment_group.clone()),
            caller_id: Some(user.into()),
            cmdb_ci: Some(request.job_name.clone()),
            correlation_id: correlation,
            ..Default::default()
        };
        let created = self.assistant_tickets.create(&fields).await?;
        self.audit(
            request,
            Uuid::new_v4(),
            ActionKind::OpenTicket,
            ActionPhase::Executed,
            ASSISTANT,
            json!({"number": created.number, "assignment_group": ticket.assignment_group}),
            None,
        )
        .await;
        Ok(created.number)
    }

    /// A reply in the proposal's thread, as the assistant.
    async fn say(&self, request: &ProposalRequest, text: &str) {
        let post = PostRequest {
            channel: request.channel,
            bot: Some(Bot::OpsAssistant),
            root_id: Some(request.root_id.clone()),
            text: Some(text.to_string()),
            card: None,
        };
        if let Err(err) = self.post(&post).await {
            warn!(error = format!("{err:#}"), "replying in the proposal's thread");
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn audit(
        &self,
        request: &ProposalRequest,
        action_id: Uuid,
        action: ActionKind,
        phase: ActionPhase,
        actor: &str,
        params: Value,
        rationale: Option<String>,
    ) {
        let event = ActionEvent {
            event_id: Uuid::new_v4(),
            ts: Utc::now(),
            action_id,
            job_name: request.job_name.clone(),
            incident_id: request.incident_id,
            signal_id: request.signal_id,
            actor: actor.to_string(),
            action,
            phase,
            params,
            rationale,
            ticket: None,
        };
        if let Err(err) = self.publisher.publish(topics::ACTIONS, &request.job_name, &event).await {
            warn!(error = format!("{err:#}"), "auditing a remediation");
        }
    }
}

/// The last two parts of a savepoint or checkpoint path.
fn short_path(path: &str) -> String {
    let parts: Vec<&str> = path.trim_end_matches('/').rsplit('/').take(2).collect();
    parts.into_iter().rev().collect::<Vec<_>>().join("/")
}
