//! Guardrails (docs/design.md §8.3): code decides whether Jev's pick may run
//! without a person. Each action has a policy row (failure classes, tiers
//! and its own conditions, in `operator.yaml`); every action also needs
//! Jev's confidence, a job the platform's pipeline deploys, and room in the
//! job's action budget. Anything that fails a check becomes an escalation.

use std::time::Duration;

use platform_model::{FailureClass, Tier};
use serde::Serialize;

use crate::{
    config::Guardrails,
    facts::Situation,
    questions::{Decision, Remediation},
};

/// What to do.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    Act(Plan),
    Observe,
    Escalate { to: Team },
}

/// An allowed action, with its parameters.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Plan {
    Rollback { from_version: String, to_version: String },
    Restart { version: String },
    ScaleUp { from: u32, to: u32 },
}

impl Plan {
    pub fn remediation(&self) -> Remediation {
        match self {
            Self::Rollback { .. } => Remediation::RollbackToPreviousVersion,
            Self::Restart { .. } => Remediation::RestartFromCheckpoint,
            Self::ScaleUp { .. } => Remediation::ScaleUp,
        }
    }

    pub fn params(&self) -> serde_json::Value {
        match self {
            Self::Rollback { from_version, to_version } => {
                serde_json::json!({"from_version": from_version, "to_version": to_version})
            }
            Self::Restart { version } => serde_json::json!({"version": version}),
            Self::ScaleUp { from, to } => serde_json::json!({"from_parallelism": from, "to_parallelism": to}),
        }
    }

    /// In words, for notes and cards.
    pub fn describe(&self) -> String {
        match self {
            Self::Rollback { from_version, to_version } => format!("roll back {from_version} → {to_version}"),
            Self::Restart { version } => format!("restart {version} from its last checkpoint"),
            Self::ScaleUp { from, to } => format!("scale up, parallelism {from} → {to}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Team {
    Owner,
    Platform,
}

/// One guardrail, and whether it passed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Check {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Assessment {
    pub verdict: Verdict,
    /// The checks that applied, in order. Empty for an escalation or
    /// `observe` that Jev picked itself.
    pub checks: Vec<Check>,
}

impl Assessment {
    #[cfg(test)]
    pub fn passed(&self) -> bool {
        matches!(self.verdict, Verdict::Act(_))
    }

    /// The failed checks, as one line.
    pub fn failures(&self) -> String {
        self.checks.iter().filter(|c| !c.passed).map(|c| c.detail.as_str()).collect::<Vec<_>>().join("; ")
    }
}

fn check(name: &'static str, passed: bool, detail: String) -> Check {
    Check { name, passed, detail }
}

fn mins(d: Duration) -> f64 {
    d.as_secs_f64() / 60.0
}

fn tier_name(tier: Option<Tier>) -> String {
    tier.and_then(|t| serde_json::to_value(t).ok()).and_then(|v| v.as_str().map(str::to_string)).unwrap_or("?".into())
}

fn class_name(class: Option<FailureClass>) -> String {
    class.and_then(|c| serde_json::to_value(c).ok()).and_then(|v| v.as_str().map(str::to_string)).unwrap_or("?".into())
}

/// Where an escalation goes: problems beneath the jobs to the platform team,
/// the rest to the owning team.
fn team(decision: &Decision, situation: &Situation) -> Team {
    if decision.remediation == Remediation::EscalateToPlatform
        || situation.class == Some(FailureClass::PlatformInfrastructure)
    {
        Team::Platform
    } else {
        Team::Owner
    }
}

/// Decides whether Jev's pick may run. `automatic` counts the job's
/// automatic actions in the budget window; `observed` how often the
/// incident has been watched already.
pub fn assess(
    g: &Guardrails,
    decision: &Decision,
    situation: &Situation,
    automatic: usize,
    observed: u32,
    observe_max: u32,
) -> Assessment {
    let escalate = |checks| Assessment { verdict: Verdict::Escalate { to: team(decision, situation) }, checks };
    let (classes, tiers) = match decision.remediation {
        Remediation::EscalateToOwner | Remediation::EscalateToPlatform => return escalate(vec![]),
        Remediation::Observe if observed < observe_max => {
            return Assessment { verdict: Verdict::Observe, checks: vec![] };
        }
        Remediation::Observe => {
            return escalate(vec![check(
                "observe",
                false,
                format!("already watched {observed} times without improvement"),
            )]);
        }
        Remediation::RollbackToPreviousVersion => {
            let r = &g.actions.rollback_to_previous_version;
            (r.classes.clone(), r.tiers.clone())
        }
        Remediation::RestartFromCheckpoint => {
            let r = &g.actions.restart_from_checkpoint;
            (r.classes.clone(), r.tiers.clone())
        }
        Remediation::ScaleUp => {
            let r = &g.actions.scale_up;
            (r.classes.clone(), r.tiers.clone())
        }
    };

    let mut checks = vec![
        check(
            "confidence",
            decision.probability >= g.min_confidence,
            format!("Jev's confidence {:.0}% (needs {:.0}%)", decision.probability * 100.0, g.min_confidence * 100.0),
        ),
        check(
            "pipeline",
            situation.catalogue.is_some(),
            if situation.catalogue.is_some() {
                "the platform's pipeline deploys the job".into()
            } else {
                "the platform's pipeline doesn't deploy the job".into()
            },
        ),
        check(
            "class",
            situation.class.is_some_and(|c| classes.contains(&c))
                && situation.class_probability >= g.min_class_confidence,
            format!(
                "failure class {} ({:.0}%) {} allowed",
                class_name(situation.class),
                situation.class_probability * 100.0,
                if situation.class.is_some_and(|c| classes.contains(&c)) { "is" } else { "isn't" }
            ),
        ),
        check(
            "tier",
            situation.tier.is_some_and(|t| tiers.contains(&t)),
            format!("tier {}", tier_name(situation.tier)),
        ),
        check(
            "budget",
            automatic < g.budget,
            format!("{automatic} automatic actions in the last {} min (budget {})", mins(g.budget_window), g.budget),
        ),
    ];

    let plan = match decision.remediation {
        Remediation::RollbackToPreviousVersion => {
            let r = &g.actions.rollback_to_previous_version;
            let deploy = situation.latest_deploy.as_ref();
            let previous = situation.previous_version.as_ref();
            let is_deploy = deploy.is_some_and(|d| d.kind == "deploy" && d.from_version.is_some());
            checks.push(check(
                "deploy age",
                is_deploy && deploy.is_some_and(|d| d.minutes_ago <= mins(r.max_deploy_age)),
                match deploy {
                    Some(d) => {
                        format!("latest {} {} min ago (at most {})", d.kind, d.minutes_ago, mins(r.max_deploy_age))
                    }
                    None => "no deploy to roll back".into(),
                },
            ));
            let first = deploy.and_then(|d| d.first_failure_minutes_after);
            checks.push(check(
                "symptoms after deploy",
                first.is_some_and(|m| m <= mins(r.symptoms_within)),
                match first {
                    Some(m) => format!("first failure {m} min after the deploy (within {})", mins(r.symptoms_within)),
                    None => "no failure since the deploy".into(),
                },
            ));
            checks.push(check(
                "previous version healthy",
                previous.is_some_and(|p| p.ran_without_failure_minutes >= mins(r.previous_healthy_for)),
                match previous {
                    Some(p) => format!(
                        "{} ran {} min without failing (needs {})",
                        p.version,
                        p.ran_without_failure_minutes,
                        mins(r.previous_healthy_for)
                    ),
                    None => "no previous version on record".into(),
                },
            ));
            match (deploy, previous) {
                (Some(d), Some(p)) => {
                    Some(Plan::Rollback { from_version: d.to_version.clone(), to_version: p.version.clone() })
                }
                _ => None,
            }
        }
        Remediation::RestartFromCheckpoint => {
            let version = situation.job.as_ref().map(|j| j.current_version.clone());
            checks.push(check(
                "running version",
                version.is_some(),
                version.clone().map_or("no current version on record".into(), |v| format!("restart {v}")),
            ));
            version.map(|version| Plan::Restart { version })
        }
        Remediation::ScaleUp => {
            let factor = g.actions.scale_up.factor;
            match &situation.capacity {
                Some(c) => {
                    let wanted = (c.parallelism * factor).min(c.max_parallelism);
                    let to = wanted.min(c.parallelism + c.free_slots);
                    checks.push(check(
                        "capacity",
                        to > c.parallelism,
                        format!(
                            "parallelism {} → {to} (tenant maximum {}, {} free slots)",
                            c.parallelism, c.max_parallelism, c.free_slots
                        ),
                    ));
                    Some(Plan::ScaleUp { from: c.parallelism, to })
                }
                None => {
                    checks.push(check("capacity", false, "the job isn't running on the cluster".into()));
                    None
                }
            }
        }
        _ => unreachable!("handled above"),
    };

    match plan {
        Some(plan) if checks.iter().all(|c| c.passed) => Assessment { verdict: Verdict::Act(plan), checks },
        _ => escalate(checks),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path};

    use platform_clients::cicd::CatalogueJob;
    use platform_model::JobKind;
    use serde_json::json;

    use super::*;
    use crate::{
        config::Config,
        facts::{Capacity, DeployFacts, PreviousVersion},
    };

    fn guardrails() -> Guardrails {
        Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/operator.yaml"))).unwrap().guardrails
    }

    fn decision(remediation: Remediation, probability: f64) -> Decision {
        Decision { remediation, probability, probabilities: BTreeMap::new() }
    }

    fn catalogue() -> CatalogueJob {
        CatalogueJob {
            tenant: "payments-eng".into(),
            owner: "@payments-eng".into(),
            tier: Tier::Tier1,
            kind: JobKind::Java,
            max_parallelism: 4,
            baseline: "1.41.0".into(),
            versions: vec![],
        }
    }

    /// F1: a code regression, failing a minute after its deploy, whose
    /// previous version ran 16 minutes without failing.
    fn f1() -> Situation {
        Situation {
            state: json!({}),
            job: None,
            catalogue: Some(catalogue()),
            tier: Some(Tier::Tier1),
            class: Some(FailureClass::CodeRegression),
            class_probability: 0.9,
            latest_deploy: Some(DeployFacts {
                kind: "deploy",
                from_version: Some("1.41.0".into()),
                to_version: "1.42.0".into(),
                minutes_ago: 1.2,
                author: "marco.diaz".into(),
                change_summary: "Normalise merchant category casing".into(),
                first_failure_minutes_after: Some(0.3),
            }),
            previous_version: Some(PreviousVersion {
                version: "1.41.0".into(),
                ran_minutes: 16.0,
                ran_without_failure_minutes: 16.0,
                failures: 0,
            }),
            capacity: Some(Capacity { parallelism: 1, max_parallelism: 4, free_slots: 5 }),
            flink_job_id: Some("j".into()),
        }
    }

    #[test]
    fn f1_rolls_back() {
        let a = assess(&guardrails(), &decision(Remediation::RollbackToPreviousVersion, 0.8), &f1(), 0, 0, 2);
        assert_eq!(
            a.verdict,
            Verdict::Act(Plan::Rollback { from_version: "1.42.0".into(), to_version: "1.41.0".into() })
        );
        assert!(a.checks.iter().all(|c| c.passed), "{a:?}");
    }

    #[test]
    fn a_rollback_escalates_when_the_failures_started_long_after_the_deploy() {
        // F2: the deploy 11 minutes before the poison records is a red herring.
        let mut s = f1();
        s.latest_deploy.as_mut().unwrap().first_failure_minutes_after = Some(11.0);
        let a = assess(&guardrails(), &decision(Remediation::RollbackToPreviousVersion, 0.8), &s, 0, 0, 2);
        assert_eq!(a.verdict, Verdict::Escalate { to: Team::Owner });
        assert!(a.failures().contains("11 min after"), "{}", a.failures());
    }

    #[test]
    fn each_guardrail_can_stop_an_action() {
        let g = guardrails();
        let rollback = decision(Remediation::RollbackToPreviousVersion, 0.8);
        let stopped = |s: Situation, d: &Decision, automatic| !assess(&g, d, &s, automatic, 0, 2).passed();
        assert!(stopped(f1(), &decision(Remediation::RollbackToPreviousVersion, 0.3), 0), "low confidence");
        assert!(stopped(f1(), &rollback, 1), "budget spent");
        assert!(stopped(Situation { catalogue: None, ..f1() }, &rollback, 0), "not in the pipeline");
        assert!(stopped(Situation { class: Some(FailureClass::StateGrowth), ..f1() }, &rollback, 0), "class");
        assert!(stopped(Situation { class_probability: 0.3, ..f1() }, &rollback, 0), "unsure class");
        let mut young = f1();
        young.previous_version.as_mut().unwrap().ran_without_failure_minutes = 4.0;
        assert!(stopped(young, &rollback, 0), "previous version barely ran");
        let mut old = f1();
        old.latest_deploy.as_mut().unwrap().minutes_ago = 180.0;
        assert!(stopped(old, &rollback, 0), "deploy too old");
        // Tier 1 jobs aren't restarted automatically.
        let restart = decision(Remediation::RestartFromCheckpoint, 0.8);
        assert!(stopped(Situation { class: Some(FailureClass::Unknown), ..f1() }, &restart, 0), "tier");
    }

    #[test]
    fn f3_scales_within_the_tenants_maximum_and_the_free_slots() {
        let g = guardrails();
        let s = || Situation {
            class: Some(FailureClass::ResourceSaturation),
            tier: Some(Tier::Tier2),
            capacity: Some(Capacity { parallelism: 2, max_parallelism: 6, free_slots: 5 }),
            ..f1()
        };
        let scale = decision(Remediation::ScaleUp, 0.7);
        assert_eq!(assess(&g, &scale, &s(), 0, 0, 2).verdict, Verdict::Act(Plan::ScaleUp { from: 2, to: 4 }));
        let few_slots =
            Situation { capacity: Some(Capacity { parallelism: 2, max_parallelism: 6, free_slots: 1 }), ..s() };
        assert_eq!(assess(&g, &scale, &few_slots, 0, 0, 2).verdict, Verdict::Act(Plan::ScaleUp { from: 2, to: 3 }));
        let at_max =
            Situation { capacity: Some(Capacity { parallelism: 6, max_parallelism: 6, free_slots: 5 }), ..s() };
        assert!(!assess(&g, &scale, &at_max, 0, 0, 2).passed());
    }

    #[test]
    fn escalations_and_observing() {
        let g = guardrails();
        let f4 = Situation { class: Some(FailureClass::StateGrowth), ..f1() };
        let a = assess(&g, &decision(Remediation::EscalateToOwner, 0.7), &f4, 0, 0, 2);
        assert_eq!(a.verdict, Verdict::Escalate { to: Team::Owner });
        let infra = Situation { class: Some(FailureClass::PlatformInfrastructure), ..f1() };
        let a = assess(&g, &decision(Remediation::ScaleUp, 0.9), &infra, 0, 0, 2);
        assert_eq!(a.verdict, Verdict::Escalate { to: Team::Platform });
        let observe = decision(Remediation::Observe, 0.6);
        assert_eq!(assess(&g, &observe, &f1(), 0, 1, 2).verdict, Verdict::Observe);
        assert_eq!(assess(&g, &observe, &f1(), 0, 2, 2).verdict, Verdict::Escalate { to: Team::Owner });
    }
}
