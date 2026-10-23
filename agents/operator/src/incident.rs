//! One incident's state machine (docs/design.md §8.3):
//!
//! ```text
//! Gather → Decide → Guardrails → Act → Verify → Close
//!              ↑         ├─► Observe ─┘ (decide again later)
//!              │         └─► Escalate
//! ```
//!
//! Each step is explicit code; Jev answers `remediation` in Decide and
//! `recovered` in Verify. Every step adds a work note to the incident's
//! ticket (which the chatops-bot echoes into the incident's thread), updates
//! the incident card, and is audited in `platform.actions`.

use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

use anyhow::Context;
use chrono::{DateTime, Utc};
use platform_clients::{
    chatops::{
        ApprovalButton, ApprovalCard, Bot, ButtonStyle, Card, Channel, ChatopsClient, Field, IncidentCard,
        IncidentStatus, PostRequest,
    },
    cicd::RequestContext,
    kafka::Publisher,
    tickets::{IncidentFields, IncidentState, TicketsClient},
};
use platform_model::{
    ActionEvent, ActionKind, ActionPhase, DeploymentOutcome, FailureClass, JobKind, Signal, Tier, topics,
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tracing::{info, warn};
use typesafe_ai::ReqwestClient;
use uuid::Uuid;

use crate::{
    config::Config,
    facts::{GUARDRAILS, Health, Situation, Sources, minutes},
    guardrails::{Assessment, Plan, Team, Verdict, assess},
    notes,
    questions::{self, Decision, Remediation},
};

/// The actor for the operator's own entries in `platform.actions`.
pub const ACTOR: &str = "operator";
/// The deploy-time argument for F4's temporary relief: state on disk.
pub const STATE_ON_DISK: &str = "--state-backend=rocksdb";

/// Where live incidents go. Dry runs have none.
pub struct Outputs {
    pub publisher: Publisher,
    pub chatops: ChatopsClient,
    pub tickets: TicketsClient,
}

#[derive(Debug, Default, Serialize)]
pub struct Stats {
    pub signals: std::sync::atomic::AtomicU64,
    pub incidents: std::sync::atomic::AtomicU64,
    pub followups: std::sync::atomic::AtomicU64,
    pub resolved: std::sync::atomic::AtomicU64,
    pub escalated: std::sync::atomic::AtomicU64,
    pub errors: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Gather,
    Decide,
    Guardrails,
    Observe,
    Act,
    Verify,
    Resolved,
    Escalated,
    Failed,
}

impl Stage {
    pub fn done(self) -> bool {
        matches!(self, Self::Resolved | Self::Escalated | Self::Failed)
    }
}

/// What `/incidents` shows, and what follow-up signals find.
#[derive(Debug, Clone, Serialize)]
pub struct Record {
    pub incident_id: Uuid,
    pub job_name: String,
    pub signal_id: Uuid,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub stage: Stage,
    pub remediation: Option<Remediation>,
    pub outcome: Option<String>,
    pub ticket: Option<String>,
    #[serde(skip)]
    pub ticket_sys_id: Option<String>,
}

/// An escalation's buttons, waiting for a person.
#[derive(Debug, Clone)]
pub struct Approval {
    pub incident_id: Uuid,
    pub signal_id: Uuid,
    pub job_name: String,
    pub owner: String,
    pub ticket_sys_id: Option<String>,
}

pub struct Operator {
    pub config: Config,
    pub sources: Sources,
    pub jev: ReqwestClient,
    pub outputs: Option<Outputs>,
    pub records: Mutex<BTreeMap<Uuid, Record>>,
    pub approvals: Mutex<BTreeMap<Uuid, Approval>>,
    pub stats: Stats,
}

/// The state machine's states. One lives per incident, so the variants'
/// sizes don't matter.
#[allow(clippy::large_enum_variant)]
enum State {
    Gather,
    Decide(Box<Situation>),
    Guardrails(Box<Situation>, Decision),
    Observe,
    Act(Box<Situation>, Plan, Uuid),
    Verify(Box<Situation>, Acted),
    Close(Box<Situation>, Acted, Health, f64),
    Escalate(Option<Box<Situation>>, Team, String),
}

/// An action that ran: when it took effect, and the job's health before.
struct Acted {
    plan: Plan,
    action_id: Uuid,
    at: DateTime<Utc>,
    before: Health,
}

/// The live parts of an incident: its card and ticket.
struct Thread {
    post_id: Option<String>,
    card: IncidentCard,
    ticket_sys_id: Option<String>,
    ticket: Option<String>,
}

impl Operator {
    /// Handles a triage signal: a raised one opens an incident, unless the
    /// job had one recently, which gets a note instead.
    pub async fn on_signal(self: &std::sync::Arc<Self>, signal: Signal) {
        if signal.disposition != platform_model::Disposition::Raise {
            return;
        }
        self.stats.signals.fetch_add(1, Ordering::Relaxed);
        let since = Utc::now() - self.config.followup;
        let recent = self
            .records
            .lock()
            .await
            .values()
            .filter(|r| r.job_name == signal.job_name && (!r.stage.done() || r.updated_at >= since))
            .max_by_key(|r| r.opened_at)
            .cloned();
        if let Some(record) = recent {
            self.stats.followups.fetch_add(1, Ordering::Relaxed);
            self.followup(&record, &signal).await;
            return;
        }
        let Some(incident_id) = signal.incident_id else { return };
        let now = Utc::now();
        self.records.lock().await.insert(
            incident_id,
            Record {
                incident_id,
                job_name: signal.job_name.clone(),
                signal_id: signal.signal_id,
                opened_at: now,
                updated_at: now,
                stage: Stage::Gather,
                remediation: None,
                outcome: None,
                ticket: None,
                ticket_sys_id: None,
            },
        );
        self.stats.incidents.fetch_add(1, Ordering::Relaxed);
        let operator = self.clone();
        tokio::spawn(async move {
            if let Err(err) = operator.run(&signal, incident_id).await {
                operator.stats.errors.fetch_add(1, Ordering::Relaxed);
                warn!(job = signal.job_name, error = format!("{err:#}"), "incident failed");
                operator.stage(incident_id, Stage::Failed, Some(format!("error: {err:#}"))).await;
            }
        });
    }

    async fn followup(&self, record: &Record, signal: &Signal) {
        info!(job = signal.job_name, incident = %record.incident_id, "another signal for an incident in hand");
        let note = format!(
            "Triage raised another signal for {} ({}, actionable {}): {} Handled in this incident.",
            signal.job_name,
            notes::class_label(signal.failure_class),
            signal.actionable.map_or("—".into(), notes::percent),
            notes::summary(signal)
        );
        if let (Some(out), Some(sys_id)) = (&self.outputs, &record.ticket_sys_id) {
            let fields = IncidentFields { work_notes: Some(note), ..Default::default() };
            if let Err(err) = out.tickets.update(sys_id, &fields).await {
                warn!(%err, "adding a follow-up note");
            }
        }
    }

    async fn stage(&self, incident_id: Uuid, stage: Stage, outcome: Option<String>) {
        if let Some(r) = self.records.lock().await.get_mut(&incident_id) {
            r.stage = stage;
            r.updated_at = Utc::now();
            if outcome.is_some() {
                r.outcome = outcome;
            }
        }
    }

    /// Runs an incident from Gather to Close or Escalate.
    async fn run(&self, signal: &Signal, incident_id: Uuid) -> anyhow::Result<()> {
        let mut thread = self.open(signal, incident_id).await?;
        let Err(err) = self.steps(signal, incident_id, &mut thread).await else { return Ok(()) };
        // A step that fails (cicd-sim or Jev unreachable, say) hands the
        // incident to a person rather than leaving it half done.
        let why = format!("the operator couldn't finish the incident ({err:#})");
        self.escalate(signal, &mut thread, None, Team::Platform, &why).await?;
        Err(err)
    }

    /// The state machine, from Gather to Close or Escalate.
    async fn steps(&self, signal: &Signal, incident_id: Uuid, thread: &mut Thread) -> anyhow::Result<()> {
        let job = signal.job_name.as_str();
        let mut observed = 0;
        let mut state = State::Gather;
        loop {
            state = match state {
                State::Gather => {
                    self.stage(incident_id, Stage::Gather, None).await;
                    let situation = self.sources.situation(&self.config.context, signal, Utc::now()).await?;
                    State::Decide(Box::new(situation))
                }
                State::Decide(situation) => {
                    self.stage(incident_id, Stage::Decide, None).await;
                    let decision = self.decide(&situation).await?;
                    if let Some(r) = self.records.lock().await.get_mut(&incident_id) {
                        r.remediation = Some(decision.remediation);
                    }
                    self.note(thread, &notes::decided(&decision)).await;
                    thread.card.fields = vec![Field::short(
                        "Jev recommends",
                        format!("{} ({})", decision.remediation.label(), notes::percent(decision.probability)),
                    )];
                    State::Guardrails(situation, decision)
                }
                State::Guardrails(situation, decision) => {
                    self.stage(incident_id, Stage::Guardrails, None).await;
                    let since = Utc::now() - self.config.guardrails.budget_window;
                    let automatic = self.sources.automatic_actions(job, since).await?;
                    let g = &self.config.guardrails;
                    let assessment = assess(g, &decision, &situation, automatic, observed, self.config.observe_max);
                    let action_id = Uuid::new_v4();
                    self.audit(
                        signal,
                        action_id,
                        decision.remediation.action(),
                        ActionPhase::Proposed,
                        ACTOR,
                        json!({"remediation": decision.remediation, "probability": decision.probability,
                               "probabilities": decision.probabilities}),
                        Some(notes::decided(&decision)),
                    )
                    .await;
                    self.note(thread, &notes::guardrails(&assessment, &decision)).await;
                    match assessment.verdict.clone() {
                        Verdict::Act(plan) => {
                            self.audit(
                                signal,
                                action_id,
                                decision.remediation.action(),
                                ActionPhase::Approved,
                                GUARDRAILS,
                                plan.params(),
                                Some(checks_line(&assessment)),
                            )
                            .await;
                            State::Act(situation, plan, action_id)
                        }
                        Verdict::Observe => State::Observe,
                        Verdict::Escalate { to } => {
                            let why = if assessment.checks.is_empty() {
                                format!("Jev recommends to {}", decision.remediation.label())
                            } else {
                                self.audit(
                                    signal,
                                    action_id,
                                    decision.remediation.action(),
                                    ActionPhase::Rejected,
                                    GUARDRAILS,
                                    json!({}),
                                    Some(assessment.failures()),
                                )
                                .await;
                                format!(
                                    "the guardrails stopped \"{}\" ({})",
                                    decision.remediation.label(),
                                    assessment.failures()
                                )
                            };
                            State::Escalate(Some(situation), to, why)
                        }
                    }
                }
                State::Observe => {
                    observed += 1;
                    self.stage(incident_id, Stage::Observe, None).await;
                    self.audit(
                        signal,
                        Uuid::new_v4(),
                        ActionKind::Observe,
                        ActionPhase::Executed,
                        ACTOR,
                        json!({}),
                        None,
                    )
                    .await;
                    self.note(
                        thread,
                        &format!("Watching for {} s, then deciding again.", self.config.observe_for.as_secs()),
                    )
                    .await;
                    tokio::time::sleep(self.config.observe_for).await;
                    State::Gather
                }
                State::Act(situation, plan, action_id) => {
                    self.stage(incident_id, Stage::Act, None).await;
                    thread.card.status = IncidentStatus::Remediating;
                    thread.card.fields.push(Field::short("Action", plan.describe()));
                    self.update_card(thread).await;
                    self.ticket_state(thread, IncidentState::InProgress).await;
                    let window = self.config.verify_after(plan.remediation());
                    let started = Utc::now();
                    let before = self.sources.health(job, started - window, started).await?;
                    match self.act(signal, &situation, &plan, action_id).await {
                        Ok((at, detail)) => {
                            self.note(thread, &notes::acted(&plan, &detail, window.as_secs())).await;
                            State::Verify(situation, Acted { plan, action_id, at, before })
                        }
                        Err(err) => {
                            warn!(job, error = format!("{err:#}"), "action failed");
                            self.audit(
                                signal,
                                action_id,
                                plan.remediation().action(),
                                ActionPhase::Failed,
                                ACTOR,
                                plan.params(),
                                Some(format!("{err:#}")),
                            )
                            .await;
                            State::Escalate(
                                Some(situation),
                                Team::Owner,
                                format!("{} failed ({err:#})", plan.describe()),
                            )
                        }
                    }
                }
                State::Verify(situation, acted) => {
                    self.stage(incident_id, Stage::Verify, None).await;
                    thread.card.status = IncidentStatus::Verifying;
                    self.update_card(thread).await;
                    let first = acted.at + self.config.verify_after(acted.plan.remediation());
                    sleep_until(first).await;
                    loop {
                        let now = Utc::now();
                        let after = self.sources.health(job, acted.at, now).await?;
                        let expected = expected(&acted.plan);
                        let state = crate::facts::verify_state(
                            job,
                            acted.plan.remediation().name(),
                            expected,
                            &acted.plan.params(),
                            minutes(acted.at, now),
                            &acted.before,
                            &after,
                        );
                        let request = questions::recovered_request(&self.config.model, state);
                        let response = self.ask(&request).await?;
                        let recovered = questions::recovered(&response)?;
                        info!(job, recovered, "verified");
                        self.note(thread, &notes::verified(recovered, &after)).await;
                        if recovered >= self.config.recovered {
                            break State::Close(situation, acted, after, recovered);
                        }
                        if (now - acted.at).to_std().unwrap_or_default() >= self.config.verify_deadline {
                            self.audit(
                                signal,
                                acted.action_id,
                                acted.plan.remediation().action(),
                                ActionPhase::Failed,
                                ACTOR,
                                json!({"recovered": recovered}),
                                Some(notes::health(&after)),
                            )
                            .await;
                            break State::Escalate(
                                Some(situation),
                                Team::Owner,
                                format!(
                                    "the job hasn't recovered {} min after: {}",
                                    minutes(acted.at, now),
                                    acted.plan.describe()
                                ),
                            );
                        }
                        tokio::time::sleep(self.config.verify_every).await;
                    }
                }
                State::Close(situation, acted, after, recovered) => {
                    self.audit(
                        signal,
                        acted.action_id,
                        acted.plan.remediation().action(),
                        ActionPhase::Verified,
                        ACTOR,
                        json!({"recovered": recovered}),
                        Some(notes::health(&after)),
                    )
                    .await;
                    let owner = owner(&situation);
                    let close_notes = notes::resolved(&acted.plan, &owner, &acted.before, &after);
                    self.resolve(thread, &acted.plan, &close_notes).await;
                    thread.card.status = IncidentStatus::Resolved;
                    thread.card.summary = close_notes.clone();
                    thread.card.fields.push(Field::short("Recovered", notes::percent(recovered)));
                    self.update_card(thread).await;
                    self.stats.resolved.fetch_add(1, Ordering::Relaxed);
                    self.stage(incident_id, Stage::Resolved, Some(acted.plan.describe())).await;
                    info!(job, plan = acted.plan.describe(), "resolved");
                    return Ok(());
                }
                State::Escalate(situation, team, why) => {
                    self.escalate(signal, thread, situation.as_deref(), team, &why).await?;
                    self.stats.escalated.fetch_add(1, Ordering::Relaxed);
                    self.stage(incident_id, Stage::Escalated, Some(why)).await;
                    return Ok(());
                }
            };
        }
    }

    /// Asks Jev, retrying a failed request (Jev's API timing out, say).
    async fn ask(&self, request: &typesafe_ai::Request) -> anyhow::Result<typesafe_ai::Response> {
        let mut attempt = 1;
        loop {
            match self.jev.evaluate(request).await {
                Ok(response) => return Ok(response),
                Err(err) if attempt < 4 => {
                    warn!(attempt, error = %err, "asking Jev; retrying");
                    tokio::time::sleep(Duration::from_secs(5 * attempt)).await;
                    attempt += 1;
                }
                Err(err) => return Err(err).context("asking Jev"),
            }
        }
    }

    /// Asks Jev which remediation the situation calls for.
    pub async fn decide(&self, situation: &Situation) -> anyhow::Result<Decision> {
        let request = questions::remediation_request(&self.config.model, situation.state.clone());
        let response = self.ask(&request).await?;
        questions::decision(&response)
    }

    /// Posts the incident card and opens the ticket.
    async fn open(&self, signal: &Signal, incident_id: Uuid) -> anyhow::Result<Thread> {
        let owner = self.sources.db.job(&signal.job_name).await?.map(|j| j.owner);
        let card = IncidentCard {
            incident_id,
            job_name: signal.job_name.clone(),
            tenant: signal.tenant.clone(),
            owner: owner.clone(),
            title: notes::title(signal),
            status: IncidentStatus::Investigating,
            summary: notes::summary(signal),
            fields: vec![],
            ticket: None,
        };
        let mut thread = Thread { post_id: None, card, ticket_sys_id: None, ticket: None };
        let Some(out) = &self.outputs else { return Ok(thread) };
        // The card first: the chatops-bot puts the ticket's card in its thread.
        match out.chatops.card(Channel::PlatformIncidents, None, Card::Incident(thread.card.clone())).await {
            Ok(p) => thread.post_id = Some(p.post_id),
            Err(err) => warn!(%err, "posting the incident card"),
        }
        let tier = self.sources.db.job(&signal.job_name).await?.map(|j| j.tier);
        let fields = IncidentFields {
            short_description: Some(notes::title(signal)),
            description: Some(notes::description(signal, crate::facts::class_probability(signal))),
            state: Some(IncidentState::New.code().into()),
            priority: Some(priority(tier).into()),
            impact: Some(priority(tier).into()),
            urgency: Some(priority(tier).into()),
            category: Some("streaming".into()),
            assignment_group: Some("platform-operations".into()),
            caller_id: Some("operator-bot".into()),
            cmdb_ci: Some(signal.job_name.clone()),
            correlation_id: Some(incident_id.to_string()),
            ..Default::default()
        };
        let ticket = out.tickets.create(&fields).await.context("opening the ticket")?;
        thread.ticket_sys_id = Some(ticket.sys_id.clone());
        thread.ticket = Some(ticket.number.clone());
        thread.card.ticket = Some(ticket.number.clone());
        if let Some(r) = self.records.lock().await.get_mut(&incident_id) {
            r.ticket = Some(ticket.number.clone());
            r.ticket_sys_id = Some(ticket.sys_id.clone());
        }
        self.audit(
            signal,
            Uuid::new_v4(),
            ActionKind::OpenTicket,
            ActionPhase::Executed,
            ACTOR,
            json!({"number": ticket.number, "assignment_group": "platform-operations"}),
            None,
        )
        .await;
        self.update_card(&thread).await;
        Ok(thread)
    }

    /// Carries out an allowed plan. Returns when it took effect, and a note
    /// on how it went.
    async fn act(
        &self,
        signal: &Signal,
        situation: &Situation,
        plan: &Plan,
        action_id: Uuid,
    ) -> anyhow::Result<(DateTime<Utc>, String)> {
        let job = signal.job_name.as_str();
        let context = RequestContext {
            triggered_by: ACTOR.into(),
            action_id: Some(action_id),
            incident_id: signal.incident_id,
            signal_id: Some(signal.signal_id),
        };
        match plan {
            Plan::Rollback { to_version, .. } => {
                let d = self.sources.cicd.rollback(job, Some(to_version), &context).await?;
                anyhow::ensure!(
                    d.outcome == DeploymentOutcome::Succeeded,
                    "the rollback failed: {}",
                    d.failure_reason.unwrap_or_default()
                );
                let from = d.restored_from.map(|p| format!(", restored from {}", short_path(&p))).unwrap_or_default();
                Ok((d.ts, from))
            }
            Plan::Restart { version } => {
                let d = self.sources.cicd.deploy(job, version, &[], &context).await?;
                anyhow::ensure!(
                    d.outcome == DeploymentOutcome::Succeeded,
                    "the restart failed: {}",
                    d.failure_reason.unwrap_or_default()
                );
                let from = d.restored_from.map(|p| format!(", restored from {}", short_path(&p))).unwrap_or_default();
                Ok((d.ts, from))
            }
            Plan::ScaleUp { to, .. } => {
                let jid = situation.flink_job_id.clone().context("the job isn't running")?;
                self.sources.flink.rescale(&jid, *to).await?;
                // The adaptive scheduler restarts the job at the new
                // parallelism from its latest checkpoint.
                let deadline = Utc::now() + chrono::Duration::minutes(3);
                loop {
                    if let Some((_, p)) = self.sources.parallelism(job).await?
                        && p >= *to
                    {
                        break;
                    }
                    anyhow::ensure!(Utc::now() < deadline, "the job didn't reach parallelism {to} within 3 minutes");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                let at = Utc::now();
                self.audit(signal, action_id, ActionKind::ScaleUp, ActionPhase::Executed, ACTOR, plan.params(), None)
                    .await;
                Ok((at, String::new()))
            }
        }
    }

    async fn escalate(
        &self,
        signal: &Signal,
        thread: &mut Thread,
        situation: Option<&Situation>,
        team: Team,
        why: &str,
    ) -> anyhow::Result<()> {
        let owner = situation.map(owner).unwrap_or_else(|| signal.tenant.clone());
        let class = situation.and_then(|s| s.class).or(signal.failure_class);
        let hint = notes::fix_hint(class, situation.map_or(&Value::Null, |s| &s.state));
        let to = notes::team_name(team, &owner);
        let note = notes::escalated(&to, why, &hint);
        let action = match team {
            Team::Owner => ActionKind::EscalateToOwner,
            Team::Platform => ActionKind::EscalateToPlatform,
        };
        let group = notes::assignment_group(team, &owner);
        self.audit(
            signal,
            Uuid::new_v4(),
            action,
            ActionPhase::Executed,
            ACTOR,
            json!({"assignment_group": group}),
            Some(why.to_string()),
        )
        .await;
        if let (Some(out), Some(sys_id)) = (&self.outputs, &thread.ticket_sys_id) {
            let fields = IncidentFields {
                assignment_group: Some(group.clone()),
                work_notes: Some(note.clone()),
                ..Default::default()
            };
            if let Err(err) = out.tickets.update(sys_id, &fields).await {
                warn!(%err, "escalating the ticket");
            }
        }
        thread.card.status = IncidentStatus::Escalated;
        thread.card.summary = format!("{}\n{hint}", notes::summary(signal));
        thread.card.fields.push(Field::long("Escalated", format!("to {to}: {why}")));
        self.update_card(thread).await;

        // Buttons for a person: acknowledge the hand-off and, where the
        // platform has a stopgap (state on disk for state growth), offer it.
        let Some(out) = &self.outputs else { return Ok(()) };
        let action_id = Uuid::new_v4();
        let mut buttons = vec![ApprovalButton {
            label: "Acknowledge".into(),
            style: ButtonStyle::Primary,
            action,
            params: json!({"assignment_group": group}),
            phase: ActionPhase::Approved,
        }];
        let stopgap = situation.and_then(|s| {
            let java = s.catalogue.as_ref().is_some_and(|c| c.kind == JobKind::Java);
            let version = s.job.as_ref().map(|j| j.current_version.clone())?;
            (java && class == Some(FailureClass::StateGrowth)).then_some(version)
        });
        if let Some(version) = &stopgap {
            buttons.push(ApprovalButton {
                label: "Restart with state on disk (temporary)".into(),
                style: ButtonStyle::Default,
                action: ActionKind::RestartFromCheckpoint,
                params: json!({"version": version, "args": [STATE_ON_DISK]}),
                phase: ActionPhase::Approved,
            });
        }
        let mut fields = vec![Field::short("Escalated to", to.clone())];
        if let Some(t) = &thread.ticket {
            fields.push(Field::short("Ticket", t.clone()));
        }
        let rationale = match &stopgap {
            Some(_) => format!(
                "{hint} Meanwhile, a restart with its state in RocksDB on local disk instead of on the JVM heap \
                 relieves memory pressure until the fix ships."
            ),
            None => hint.clone(),
        };
        let card = ApprovalCard {
            action_id,
            job_name: signal.job_name.clone(),
            incident_id: signal.incident_id,
            signal_id: Some(signal.signal_id),
            title: format!("{}: needs {to}", signal.job_name),
            rationale,
            proposed_by: "operator-bot".into(),
            fields,
            buttons,
        };
        self.approvals.lock().await.insert(
            action_id,
            Approval {
                incident_id: thread.card.incident_id,
                signal_id: signal.signal_id,
                job_name: signal.job_name.clone(),
                owner: to.clone(),
                ticket_sys_id: thread.ticket_sys_id.clone(),
            },
        );
        let request = PostRequest {
            channel: Channel::PlatformIncidents,
            bot: Some(Bot::OperatorBot),
            root_id: thread.post_id.clone(),
            text: None,
            card: Some(Card::Approval(card)),
        };
        if let Err(err) = out.chatops.post(&request).await {
            warn!(%err, "posting the escalation card");
        }
        Ok(())
    }

    /// A person's decision on an escalation's buttons (from
    /// `platform.actions`): note it and, for the stopgap, carry it out.
    pub async fn on_action(&self, event: ActionEvent) {
        let Some(user) = event.actor.strip_prefix("user:") else { return };
        if event.phase != ActionPhase::Approved {
            return;
        }
        let Some(approval) = self.approvals.lock().await.remove(&event.action_id) else { return };
        let ticket = approval.ticket_sys_id.as_deref();
        match event.action {
            ActionKind::RestartFromCheckpoint => {
                let version = event.params["version"].as_str().unwrap_or_default().to_string();
                let args: Vec<String> = event.params["args"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                    .unwrap_or_default();
                let fields = IncidentFields {
                    state: Some(IncidentState::InProgress.code().into()),
                    work_notes: Some(format!(
                        "@{user} approved a temporary restart of {version} with its state on disk. Restarting."
                    )),
                    ..Default::default()
                };
                self.update_ticket(ticket, fields).await;
                let context = RequestContext {
                    triggered_by: format!("user:{user}"),
                    action_id: Some(event.action_id),
                    incident_id: Some(approval.incident_id),
                    signal_id: Some(approval.signal_id),
                };
                let note = match self.sources.cicd.deploy(&approval.job_name, &version, &args, &context).await {
                    Ok(d) if d.outcome == DeploymentOutcome::Succeeded => format!(
                        "Restarted {version} with its state on disk{}. This buys time; {} still owns the fix.",
                        d.restored_from.map(|p| format!(", from {}", short_path(&p))).unwrap_or_default(),
                        approval.owner
                    ),
                    Ok(d) => format!("The restart failed: {}", d.failure_reason.unwrap_or_default()),
                    Err(err) => format!("The restart failed: {err:#}"),
                };
                info!(job = approval.job_name, user, note, "approved stopgap");
                self.update_ticket(ticket, IncidentFields { work_notes: Some(note), ..Default::default() }).await;
            }
            _ => {
                let fields = IncidentFields {
                    state: Some(IncidentState::InProgress.code().into()),
                    assigned_to: Some(user.to_string()),
                    work_notes: Some(format!("@{user} acknowledged the escalation to {}.", approval.owner)),
                    ..Default::default()
                };
                self.update_ticket(ticket, fields).await;
            }
        }
    }

    async fn update_ticket(&self, sys_id: Option<&str>, fields: IncidentFields) {
        let (Some(out), Some(sys_id)) = (&self.outputs, sys_id) else { return };
        if let Err(err) = out.tickets.update(sys_id, &fields).await {
            warn!(%err, "updating the ticket");
        }
    }

    async fn resolve(&self, thread: &Thread, plan: &Plan, close_notes: &str) {
        let (Some(out), Some(sys_id)) = (&self.outputs, &thread.ticket_sys_id) else { return };
        let close_code = match plan {
            Plan::Rollback { .. } => "Solved (Work Around)",
            _ => "Solved (Permanently)",
        };
        let fields = IncidentFields {
            state: Some(IncidentState::Resolved.code().into()),
            close_code: Some(close_code.into()),
            close_notes: Some(close_notes.into()),
            work_notes: Some(format!("Resolved: {close_notes}")),
            ..Default::default()
        };
        if let Err(err) = out.tickets.update(sys_id, &fields).await {
            warn!(%err, "resolving the ticket");
        }
    }

    async fn ticket_state(&self, thread: &Thread, state: IncidentState) {
        let (Some(out), Some(sys_id)) = (&self.outputs, &thread.ticket_sys_id) else { return };
        let fields = IncidentFields { state: Some(state.code().into()), ..Default::default() };
        if let Err(err) = out.tickets.update(sys_id, &fields).await {
            warn!(%err, "updating the ticket's state");
        }
    }

    /// A work note on the ticket, which the chatops-bot echoes into the
    /// incident's thread.
    async fn note(&self, thread: &Thread, text: &str) {
        info!(job = thread.card.job_name, note = text, "note");
        let (Some(out), Some(sys_id)) = (&self.outputs, &thread.ticket_sys_id) else { return };
        let fields = IncidentFields { work_notes: Some(text.to_string()), ..Default::default() };
        if let Err(err) = out.tickets.update(sys_id, &fields).await {
            warn!(%err, "adding a work note");
        }
    }

    async fn update_card(&self, thread: &Thread) {
        let (Some(out), Some(post_id)) = (&self.outputs, &thread.post_id) else { return };
        let request = PostRequest {
            channel: Channel::PlatformIncidents,
            bot: None,
            root_id: None,
            text: None,
            card: Some(Card::Incident(thread.card.clone())),
        };
        if let Err(err) = out.chatops.update(post_id, &request).await {
            warn!(%err, "updating the incident card");
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn audit(
        &self,
        signal: &Signal,
        action_id: Uuid,
        action: ActionKind,
        phase: ActionPhase,
        actor: &str,
        params: Value,
        rationale: Option<String>,
    ) {
        let Some(out) = &self.outputs else { return };
        let ticket = match signal.incident_id {
            Some(id) => self.records.lock().await.get(&id).and_then(|r| r.ticket.clone()),
            None => None,
        };
        let event = ActionEvent {
            event_id: Uuid::new_v4(),
            ts: Utc::now(),
            action_id,
            job_name: signal.job_name.clone(),
            incident_id: signal.incident_id,
            signal_id: Some(signal.signal_id),
            actor: actor.to_string(),
            action,
            phase,
            params,
            rationale,
            ticket,
        };
        if let Err(err) = out.publisher.publish(topics::ACTIONS, &signal.job_name, &event).await {
            warn!(error = format!("{err:#}"), "auditing an action");
        }
    }
}

fn checks_line(assessment: &Assessment) -> String {
    assessment.checks.iter().map(|c| c.detail.as_str()).collect::<Vec<_>>().join("; ")
}

fn owner(situation: &Situation) -> String {
    situation
        .job
        .as_ref()
        .map(|j| j.owner.clone())
        .or_else(|| situation.catalogue.as_ref().map(|c| c.owner.clone()))
        .unwrap_or_else(|| "the owning team".into())
}

/// ServiceNow priority by tier: tier 1 is high, tier 3 low.
fn priority(tier: Option<Tier>) -> &'static str {
    match tier {
        Some(Tier::Tier1) => "2",
        Some(Tier::Tier2) => "3",
        Some(Tier::Tier3) | None => "4",
    }
}

/// What recovery looks like after each action, for `recovered`.
fn expected(plan: &Plan) -> &'static str {
    match plan {
        Plan::Rollback { .. } => {
            "The previous version runs without the failures the bad version had, and checkpoints complete again."
        }
        Plan::Restart { .. } => "The job runs again without failing, and checkpoints complete.",
        Plan::ScaleUp { .. } => {
            "With more parallelism the job keeps up: lag stops growing and falls, and backpressure eases."
        }
    }
}

/// The last two parts of a checkpoint or savepoint path.
fn short_path(path: &str) -> String {
    let parts: Vec<&str> = path.trim_end_matches('/').rsplit('/').take(2).collect();
    parts.into_iter().rev().collect::<Vec<_>>().join("/")
}

async fn sleep_until(at: DateTime<Utc>) {
    if let Ok(wait) = (at - Utc::now()).to_std() {
        tokio::time::sleep(wait).await;
    }
}
