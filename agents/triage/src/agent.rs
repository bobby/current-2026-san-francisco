//! One judgment: gather the evidence, ask Jev, apply the policy, and (when
//! live) post the card, record the incident and publish the signal.

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use chrono::{DateTime, Utc};
use platform_clients::{
    chatops::{Card, Channel, ChatopsClient, SignalCard},
    kafka::Publisher,
};
use platform_model::{AlertEvent, Deployment, Disposition, Signal, topics};
use serde::Serialize;
use serde_json::json;
use tokio::sync::Mutex;
use typesafe_ai::ReqwestClient;
use uuid::Uuid;

use crate::{
    config::Config,
    evidence::Sources,
    incidents::Incidents,
    policy::{Decision, decide},
    questions::{self, Judgment},
    summary::summary,
};

/// Where live verdicts go. Replays have none.
pub struct Outputs {
    pub publisher: Publisher,
    pub chatops: ChatopsClient,
}

/// Counters for `GET /stats`: alerts in, and what became of them.
#[derive(Debug, Default, Serialize)]
pub struct Stats {
    pub alerts_in: AtomicU64,
    pub judgments: AtomicU64,
    pub raised: AtomicU64,
    pub attached: AtomicU64,
    pub suppressed: AtomicU64,
    /// Alerts in suppressed groups.
    pub alerts_suppressed: AtomicU64,
    pub errors: AtomicU64,
}

pub struct Triage {
    pub config: Config,
    pub sources: Sources,
    pub jev: ReqwestClient,
    pub incidents: Mutex<Incidents>,
    pub outputs: Option<Outputs>,
    pub stats: Stats,
}

pub struct Verdict {
    pub signal: Signal,
    pub judgment: Judgment,
    pub deployment: Option<Deployment>,
    pub summary: String,
}

impl Triage {
    /// Judges one job's group of alerts as of `at`, without side effects.
    /// `earlier` are signals to count as the job's triage history besides
    /// the ops DB's (a replay's own verdicts).
    pub async fn judge(
        &self,
        job_name: &str,
        alerts: &[AlertEvent],
        earlier: &[Signal],
        at: DateTime<Utc>,
    ) -> anyhow::Result<Verdict> {
        let gathered =
            self.sources.gather(&self.config, job_name, alerts, earlier, at).await.context("gathering evidence")?;
        let candidates = self.incidents.lock().await.candidates(job_name, &gathered.tenant, at);
        let evidence = gathered.evidence.with_open_incidents(&candidates, at);

        let request = questions::request(&self.config.model, &evidence)?;
        let response = self.jev.evaluate(&request).await.context("asking Jev")?;
        let judgment = questions::judgment(&response)?;
        let decision = decide(&judgment, &self.config.policy, &candidates);
        let (disposition, incident_id) = match decision {
            Decision::Raise => (Disposition::Raise, Some(Uuid::new_v4())),
            Decision::Attach(id) => (Disposition::Attach, Some(id)),
            Decision::Suppress => (Disposition::Suppress, None),
        };
        let summary = summary(&evidence);
        let signal = Signal {
            signal_id: Uuid::new_v4(),
            ts: at,
            job_name: job_name.to_string(),
            tenant: gathered.tenant,
            alert_event_ids: alerts.iter().map(|a| a.event_id).collect(),
            disposition,
            incident_id,
            failure_class: Some(judgment.failure_class),
            actionable: Some(judgment.actionable),
            user_impact: Some(judgment.user_impact / 3.0),
            deploy_related: judgment.deploy_related,
            deployment_event_id: gathered.deployment.as_ref().map(|d| d.event_id),
            judgment: json!({
                "summary": summary,
                "model": response.model,
                "state": evidence,
                "questions": request.questions,
                "answers": response.answers,
                "usage": response.usage,
            }),
        };
        Ok(Verdict { signal, judgment, deployment: gathered.deployment, summary })
    }

    /// Waits until the job has settled after a recent failure (see
    /// [`Sources::settled`]).
    pub async fn settle(&self, job_name: &str) -> anyhow::Result<()> {
        let due = Utc::now();
        while self.sources.settled(&self.config, job_name, due, Utc::now()).await?.is_none() {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
        Ok(())
    }

    /// Judges a group now and acts on the verdict.
    pub async fn handle(&self, job_name: &str, alerts: &[AlertEvent]) -> anyhow::Result<Verdict> {
        let mut verdict = self.judge(job_name, alerts, &[], Utc::now()).await?;
        let s = &mut verdict.signal;
        let counter = match s.disposition {
            Disposition::Raise => &self.stats.raised,
            Disposition::Attach => &self.stats.attached,
            Disposition::Suppress => {
                self.stats.alerts_suppressed.fetch_add(alerts.len() as u64, Ordering::Relaxed);
                &self.stats.suppressed
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.stats.judgments.fetch_add(1, Ordering::Relaxed);

        let Some(out) = &self.outputs else { return Ok(verdict) };
        // Raised signals get a card in the channel; attached ones go in their
        // incident's thread; suppressed ones stay out of chat.
        let root = match (s.disposition, s.incident_id) {
            (Disposition::Attach, Some(id)) => self.incidents.lock().await.get(id).and_then(|i| i.post_id.clone()),
            _ => None,
        };
        let card = || Card::Signal(SignalCard { signal: s.clone(), deployment: verdict.deployment.clone() });
        let post_id = match s.disposition {
            Disposition::Raise => post(&out.chatops, None, card()).await,
            Disposition::Attach if root.is_some() => post(&out.chatops, root.as_deref(), card()).await,
            _ => None,
        };
        if let (Disposition::Raise, Some(id)) = (s.disposition, &post_id) {
            s.judgment["post_id"] = json!(id);
        }
        self.incidents.lock().await.record(
            s,
            &verdict.summary,
            post_id.filter(|_| s.disposition == Disposition::Raise),
        );
        out.publisher.publish(topics::SIGNALS, &s.job_name, s).await.context("publishing the signal")?;
        Ok(verdict)
    }
}

async fn post(chatops: &ChatopsClient, root: Option<&str>, card: Card) -> Option<String> {
    match chatops.card(Channel::PlatformSignals, root, card).await {
        Ok(p) => Some(p.post_id),
        Err(err) => {
            tracing::warn!(%err, "posting a signal card");
            None
        }
    }
}
