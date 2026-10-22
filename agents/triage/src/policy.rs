//! Code policy (docs/design.md §8.1): Jev's answers and the thresholds in
//! `triage.yaml` decide each group's disposition.
//!
//! 1. Attach to an open incident when `same_incident` picks one confidently.
//! 2. Suppress when the failure class is confidently `transient_or_expected`.
//! 3. Raise when `actionable` clears the bar, or clears a lower one with a
//!    high user impact, or with a confident failure class that only a fix
//!    resolves (a code regression, bad input data, state growth, a schema
//!    mismatch).
//! 4. Otherwise suppress.

use platform_model::FailureClass;
use uuid::Uuid;

use crate::{
    config::Policy,
    incidents::Incident,
    questions::{Judgment, NEW_INCIDENT},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Suppress,
    Attach(Uuid),
    Raise,
}

pub fn decide(judgment: &Judgment, policy: &Policy, candidates: &[Incident]) -> Decision {
    if let Some((choice, p)) = &judgment.same_incident
        && choice != NEW_INCIDENT
        && *p >= policy.attach
        && let Some(incident) = candidates.iter().find(|i| i.key() == *choice)
    {
        return Decision::Attach(incident.id);
    }
    if judgment.class_probability(FailureClass::TransientOrExpected) >= policy.transient_veto {
        return Decision::Suppress;
    }
    let needs_fix = matches!(
        judgment.failure_class,
        FailureClass::CodeRegression
            | FailureClass::BadInputData
            | FailureClass::StateGrowth
            | FailureClass::SchemaMismatch
    ) && judgment.class_probability(judgment.failure_class) >= policy.fix_class;
    let raise = judgment.actionable >= policy.raise
        || (judgment.actionable >= policy.impact_raise && judgment.user_impact >= policy.impact)
        || (judgment.actionable >= policy.fix_class_raise && needs_fix);
    if raise { Decision::Raise } else { Decision::Suppress }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;

    use super::*;

    fn policy() -> Policy {
        Policy {
            raise: 0.6,
            impact_raise: 0.35,
            impact: 2.5,
            fix_class: 0.7,
            fix_class_raise: 0.4,
            transient_veto: 0.5,
            attach: 0.5,
        }
    }

    fn judgment(actionable: f64, class: FailureClass, p_class: f64, impact: f64) -> Judgment {
        Judgment {
            actionable,
            failure_class: class,
            class_probabilities: BTreeMap::from([(crate::questions::class_name(class).to_string(), p_class)]),
            user_impact: impact,
            deploy_related: None,
            same_incident: None,
        }
    }

    fn incident() -> Incident {
        Incident {
            id: Uuid::new_v4(),
            job_name: "payments-enrichment".into(),
            tenant: "payments-eng".into(),
            class: Some(FailureClass::CodeRegression),
            opened_at: Utc::now(),
            last_seen: Utc::now(),
            summary: String::new(),
            post_id: None,
        }
    }

    #[test]
    fn actionable_problems_are_raised() {
        let j = judgment(0.9, FailureClass::CodeRegression, 0.8, 3.0);
        assert_eq!(decide(&j, &policy(), &[]), Decision::Raise);
    }

    #[test]
    fn a_borderline_problem_with_high_impact_is_raised() {
        let j = judgment(0.4, FailureClass::ResourceSaturation, 0.6, 2.7);
        assert_eq!(decide(&j, &policy(), &[]), Decision::Raise);
        let j = judgment(0.4, FailureClass::ResourceSaturation, 0.6, 1.0);
        assert_eq!(decide(&j, &policy(), &[]), Decision::Suppress);
    }

    #[test]
    fn a_confident_class_that_needs_a_fix_lowers_the_bar() {
        let j = judgment(0.52, FailureClass::StateGrowth, 0.9, 1.5);
        assert_eq!(decide(&j, &policy(), &[]), Decision::Raise);
        // Not for a class a fix doesn't address, or an unsure one.
        let j = judgment(0.52, FailureClass::PlatformInfrastructure, 0.9, 1.5);
        assert_eq!(decide(&j, &policy(), &[]), Decision::Suppress);
        let j = judgment(0.52, FailureClass::StateGrowth, 0.5, 1.5);
        assert_eq!(decide(&j, &policy(), &[]), Decision::Suppress);
    }

    #[test]
    fn transient_conditions_are_suppressed_even_when_actionable() {
        let j = judgment(0.6, FailureClass::TransientOrExpected, 0.7, 1.0);
        assert_eq!(decide(&j, &policy(), &[]), Decision::Suppress);
    }

    #[test]
    fn alerts_attach_to_the_open_incident_jev_picks() {
        let open = incident();
        let mut j = judgment(0.2, FailureClass::TransientOrExpected, 0.9, 0.0);
        j.same_incident = Some((open.key(), 0.8));
        assert_eq!(decide(&j, &policy(), std::slice::from_ref(&open)), Decision::Attach(open.id));
        // An unsure pick, or "none", doesn't attach.
        j.same_incident = Some((open.key(), 0.4));
        assert_eq!(decide(&j, &policy(), std::slice::from_ref(&open)), Decision::Suppress);
        j.same_incident = Some((NEW_INCIDENT.into(), 0.9));
        assert_eq!(decide(&j, &policy(), &[open]), Decision::Suppress);
    }
}
