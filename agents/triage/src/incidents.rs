//! Open incidents: what a raised signal starts and later signals can attach
//! to. An incident stays open for `incident_ttl` after its latest signal.
//! They're rebuilt from the ops DB's signals when the agent starts.

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, Utc};
use platform_model::{Disposition, FailureClass, Signal};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct Incident {
    pub id: Uuid,
    pub job_name: String,
    pub tenant: String,
    pub class: Option<FailureClass>,
    pub opened_at: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub summary: String,
    /// The raised signal's card in `#platform-signals`; attached signals go
    /// in its thread.
    pub post_id: Option<String>,
}

impl Incident {
    /// Its name as a `same_incident` option.
    pub fn key(&self) -> String {
        format!("incident_{}", &self.id.simple().to_string()[..8])
    }
}

#[derive(Debug, Default)]
pub struct Incidents {
    ttl: Duration,
    open: BTreeMap<Uuid, Incident>,
}

impl Incidents {
    pub fn new(ttl: Duration) -> Self {
        Self { ttl, open: BTreeMap::new() }
    }

    /// Rebuilds the open incidents from signals, oldest first.
    pub fn from_signals(ttl: Duration, signals: &[Signal]) -> Self {
        let mut incidents = Self::new(ttl);
        for s in signals {
            let post_id = s.judgment.get("post_id").and_then(|v| v.as_str()).map(str::to_string);
            let summary = s.judgment.get("summary").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            incidents.record(s, &summary, post_id);
        }
        incidents
    }

    /// Incidents open at `at` for this job, then for others of its tenant,
    /// newest first: the candidates for `same_incident`.
    pub fn candidates(&self, job_name: &str, tenant: &str, at: DateTime<Utc>) -> Vec<Incident> {
        let mut open: Vec<_> = self
            .open
            .values()
            .filter(|i| self.is_open(i, at) && (i.job_name == job_name || (!tenant.is_empty() && i.tenant == tenant)))
            .cloned()
            .collect();
        open.sort_by(|a, b| {
            (b.job_name == job_name).cmp(&(a.job_name == job_name)).then(b.last_seen.cmp(&a.last_seen))
        });
        open.truncate(5);
        open
    }

    fn is_open(&self, incident: &Incident, at: DateTime<Utc>) -> bool {
        within(incident.last_seen, at, self.ttl)
    }

    pub fn get(&self, id: Uuid) -> Option<&Incident> {
        self.open.get(&id)
    }

    /// Notes a signal: a raise opens an incident, an attach keeps one open.
    pub fn record(&mut self, signal: &Signal, summary: &str, post_id: Option<String>) {
        let Some(id) = signal.incident_id else { return };
        match signal.disposition {
            Disposition::Raise => {
                self.open.insert(
                    id,
                    Incident {
                        id,
                        job_name: signal.job_name.clone(),
                        tenant: signal.tenant.clone(),
                        class: signal.failure_class,
                        opened_at: signal.ts,
                        last_seen: signal.ts,
                        summary: summary.to_string(),
                        post_id,
                    },
                );
            }
            Disposition::Attach => {
                if let Some(incident) = self.open.get_mut(&id) {
                    incident.last_seen = incident.last_seen.max(signal.ts);
                }
            }
            Disposition::Suppress => {}
        }
        let ttl = self.ttl;
        self.open.retain(|_, i| within(i.last_seen, signal.ts, ttl));
    }
}

/// `at` is no more than `ttl` after `seen` (or before it).
fn within(seen: DateTime<Utc>, at: DateTime<Utc>, ttl: Duration) -> bool {
    (at - seen).to_std().map_or(true, |age| age <= ttl)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(job: &str, tenant: &str, disposition: Disposition, incident: Uuid, minute: i64) -> Signal {
        Signal {
            signal_id: Uuid::new_v4(),
            ts: DateTime::from_timestamp(minute * 60, 0).unwrap(),
            job_name: job.into(),
            tenant: tenant.into(),
            alert_event_ids: vec![],
            disposition,
            incident_id: Some(incident),
            failure_class: Some(FailureClass::CodeRegression),
            actionable: None,
            user_impact: None,
            deploy_related: None,
            deployment_event_id: None,
            judgment: serde_json::json!({"summary": "NPE storm", "post_id": "p1"}),
        }
    }

    #[test]
    fn incidents_open_on_raise_stay_open_on_attach_and_expire() {
        let ttl = Duration::from_secs(30 * 60);
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        let incidents = Incidents::from_signals(
            ttl,
            &[
                signal("payments-enrichment", "payments-eng", Disposition::Raise, a, 0),
                signal("payments-enrichment", "payments-eng", Disposition::Attach, a, 20),
                signal("ledger", "finance", Disposition::Raise, b, 1),
            ],
        );
        let at = |m: i64| DateTime::from_timestamp(m * 60, 0).unwrap();
        let open = incidents.candidates("payments-enrichment", "payments-eng", at(45));
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].post_id.as_deref(), Some("p1"));
        assert_eq!(open[0].summary, "NPE storm");
        // Another tenant's incident isn't a candidate, and expires.
        assert!(incidents.candidates("ledger", "finance", at(45)).is_empty());
        assert!(incidents.candidates("payments-enrichment", "payments-eng", at(51)).is_empty());
        // Same tenant, other job: still a candidate.
        assert_eq!(incidents.candidates("payments-router", "payments-eng", at(30)).len(), 1);
        assert!(open[0].key().starts_with("incident_"));
    }
}
