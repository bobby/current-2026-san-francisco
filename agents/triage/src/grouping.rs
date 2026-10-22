//! Groups firing alerts per job: the first alert opens a group, and the
//! group is judged once `group_window` has passed, with every alert for the
//! job that arrived meanwhile. A storm of rules about one problem becomes one
//! judgment. While a job's group is being judged, its new alerts wait in the
//! next group, so a job's judgments happen in order.
//!
//! Alertmanager reports an alert once when it starts firing, so a problem
//! that worsens slowly (state growth, a stalled consumer) could be judged
//! only while it still looked harmless. [`Watch`] keeps the alerts still
//! firing, and when a job's latest verdict was `suppress` it judges them
//! again after `recheck`, then twice as long, and so on.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use platform_model::{AlertEvent, AlertStatus, Disposition};

#[derive(Debug)]
struct Group {
    opened: DateTime<Utc>,
    alerts: Vec<AlertEvent>,
}

#[derive(Debug)]
pub struct Grouper {
    window: chrono::Duration,
    pending: BTreeMap<String, Group>,
}

/// A group ready to judge, as of `due`.
#[derive(Debug, PartialEq)]
pub struct Due {
    pub job_name: String,
    pub alerts: Vec<AlertEvent>,
    pub due: DateTime<Utc>,
}

impl Grouper {
    pub fn new(window: std::time::Duration) -> Self {
        Self { window: chrono::Duration::from_std(window).expect("window fits"), pending: BTreeMap::new() }
    }

    pub fn push(&mut self, job_name: &str, alert: AlertEvent, now: DateTime<Utc>) {
        self.pending
            .entry(job_name.to_string())
            .or_insert_with(|| Group { opened: now, alerts: Vec::new() })
            .alerts
            .push(alert);
    }

    /// Takes the groups whose window has passed by `now`, except those of
    /// `busy` jobs, oldest first.
    pub fn due(&mut self, now: DateTime<Utc>, busy: &HashSet<String>) -> Vec<Due> {
        let ready: Vec<String> = self
            .pending
            .iter()
            .filter(|(job, g)| now - g.opened >= self.window && !busy.contains(*job))
            .map(|(job, _)| job.clone())
            .collect();
        let mut due: Vec<Due> = ready
            .into_iter()
            .filter_map(|job| {
                let g = self.pending.remove(&job)?;
                Some(Due { job_name: job, alerts: g.alerts, due: g.opened + self.window })
            })
            .collect();
        due.sort_by_key(|d| d.due);
        due
    }
}

/// The alerts still firing, and when each job was last judged.
#[derive(Debug)]
pub struct Watch {
    recheck: chrono::Duration,
    /// By fingerprint.
    firing: BTreeMap<String, AlertEvent>,
    /// Per job: when it was last judged (or a recheck was queued), and how
    /// many suppress verdicts in a row it has had.
    last: BTreeMap<String, (DateTime<Utc>, u32)>,
}

impl Watch {
    pub fn new(recheck: std::time::Duration) -> Self {
        Self {
            recheck: chrono::Duration::from_std(recheck).expect("recheck fits"),
            firing: BTreeMap::new(),
            last: BTreeMap::new(),
        }
    }

    pub fn observe(&mut self, alert: &AlertEvent) {
        match alert.status {
            AlertStatus::Firing => {
                self.firing.insert(alert.fingerprint.clone(), alert.clone());
                // A new alert is new evidence: the job's backoff starts over.
                if let Some((_, streak)) = alert.job_name.as_ref().and_then(|j| self.last.get_mut(j)) {
                    *streak = 0;
                }
            }
            AlertStatus::Resolved => {
                self.firing.remove(&alert.fingerprint);
            }
        }
    }

    /// Notes a verdict. Only a run of suppress verdicts leads to rechecks.
    pub fn judged(&mut self, job_name: &str, at: DateTime<Utc>, disposition: Disposition) {
        match disposition {
            Disposition::Suppress => {
                let streak = self.last.get(job_name).map_or(0, |(_, n)| *n);
                self.last.insert(job_name.to_string(), (at, streak + 1));
            }
            _ => {
                self.last.remove(job_name);
            }
        }
    }

    /// The still-firing alerts of jobs due a recheck at `now`, except jobs
    /// that are `busy` or already have a group pending.
    pub fn due(
        &mut self,
        now: DateTime<Utc>,
        busy: &HashSet<String>,
        grouper: &Grouper,
    ) -> Vec<(String, Vec<AlertEvent>)> {
        let mut by_job: BTreeMap<&str, Vec<AlertEvent>> = BTreeMap::new();
        for a in self.firing.values() {
            if let Some(job) = &a.job_name {
                by_job.entry(job).or_default().push(a.clone());
            }
        }
        let mut due = Vec::new();
        for (job, alerts) in by_job {
            let Some((at, streak)) = self.last.get_mut(job) else { continue };
            let wait = self.recheck * 2i32.pow(streak.saturating_sub(1).min(6));
            if now - *at >= wait && !busy.contains(job) && !grouper.pending.contains_key(job) {
                *at = now;
                due.push((job.to_string(), alerts));
            }
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use platform_model::AlertStatus;

    use super::*;

    fn alert(name: &str, secs: i64) -> AlertEvent {
        let ts = DateTime::from_timestamp(secs, 0).unwrap();
        AlertEvent {
            event_id: uuid::Uuid::new_v4(),
            ts,
            status: AlertStatus::Firing,
            alertname: name.into(),
            fingerprint: name.into(),
            severity: None,
            job_name: Some("job".into()),
            tenant: None,
            starts_at: ts,
            ends_at: None,
            labels: Default::default(),
            annotations: Default::default(),
            generator_url: None,
        }
    }

    #[test]
    fn suppressed_alerts_still_firing_are_rechecked_with_backoff() {
        let at = |s| DateTime::from_timestamp(s, 0).unwrap();
        let grouper = Grouper::new(Duration::from_secs(45));
        let mut w = Watch::new(Duration::from_secs(300));
        let slow = alert("FlinkCheckpointSlow", 0);
        w.observe(&slow);
        w.judged("job", at(45), Disposition::Suppress);
        assert!(w.due(at(300), &HashSet::new(), &grouper).is_empty());
        assert_eq!(w.due(at(345), &HashSet::new(), &grouper).len(), 1);
        // Suppressed again: the next recheck waits twice as long.
        w.judged("job", at(350), Disposition::Suppress);
        assert!(w.due(at(650), &HashSet::new(), &grouper).is_empty());
        assert_eq!(w.due(at(950), &HashSet::new(), &grouper).len(), 1);
        // Raised, or resolved: no more rechecks.
        w.judged("job", at(960), Disposition::Raise);
        assert!(w.due(at(5000), &HashSet::new(), &grouper).is_empty());
        w.judged("job", at(5000), Disposition::Suppress);
        w.observe(&AlertEvent { status: AlertStatus::Resolved, ..slow });
        assert!(w.due(at(9000), &HashSet::new(), &grouper).is_empty());
    }

    #[test]
    fn a_storm_is_one_group_and_busy_jobs_wait() {
        let at = |s| DateTime::from_timestamp(s, 0).unwrap();
        let mut g = Grouper::new(Duration::from_secs(45));
        g.push("job", alert("FlinkJobRestarted", 0), at(0));
        g.push("job", alert("FlinkCheckpointFailed", 10), at(10));
        g.push("other", alert("FlinkCheckpointSlow", 30), at(30));
        assert!(g.due(at(44), &HashSet::new()).is_empty());

        let busy = HashSet::from(["job".to_string()]);
        assert!(g.due(at(50), &busy).is_empty());

        let due = g.due(at(80), &HashSet::new());
        assert_eq!(
            due.iter().map(|d| (d.job_name.as_str(), d.alerts.len())).collect::<Vec<_>>(),
            [("job", 2), ("other", 1)]
        );
        assert_eq!(due[0].due, at(45));
        assert!(g.pending.is_empty());
    }
}
