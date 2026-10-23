//! The questions Jev answers for the operator (docs/design.md §8.3):
//! `remediation`, which fix the evidence calls for, and `recovered`, whether
//! the job is healthy again after it. Code decides what happens with the
//! answers.

use std::collections::BTreeMap;

use anyhow::Context;
use platform_model::ActionKind;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use typesafe_ai::{NoulCriteria, Question, Request, Response};

/// The remediations Jev chooses between.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Remediation {
    RollbackToPreviousVersion,
    RestartFromCheckpoint,
    ScaleUp,
    EscalateToOwner,
    EscalateToPlatform,
    Observe,
}

impl Remediation {
    pub const ALL: [Self; 6] = [
        Self::RollbackToPreviousVersion,
        Self::RestartFromCheckpoint,
        Self::ScaleUp,
        Self::EscalateToOwner,
        Self::EscalateToPlatform,
        Self::Observe,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::RollbackToPreviousVersion => "rollback_to_previous_version",
            Self::RestartFromCheckpoint => "restart_from_checkpoint",
            Self::ScaleUp => "scale_up",
            Self::EscalateToOwner => "escalate_to_owner",
            Self::EscalateToPlatform => "escalate_to_platform",
            Self::Observe => "observe",
        }
    }

    /// In words, for notes and cards.
    pub fn label(self) -> &'static str {
        match self {
            Self::RollbackToPreviousVersion => "roll back to the previous version",
            Self::RestartFromCheckpoint => "restart from the last checkpoint",
            Self::ScaleUp => "scale up",
            Self::EscalateToOwner => "escalate to the owning team",
            Self::EscalateToPlatform => "escalate to the platform team",
            Self::Observe => "keep watching",
        }
    }

    pub fn action(self) -> ActionKind {
        match self {
            Self::RollbackToPreviousVersion => ActionKind::RollbackToPreviousVersion,
            Self::RestartFromCheckpoint => ActionKind::RestartFromCheckpoint,
            Self::ScaleUp => ActionKind::ScaleUp,
            Self::EscalateToOwner => ActionKind::EscalateToOwner,
            Self::EscalateToPlatform => ActionKind::EscalateToPlatform,
            Self::Observe => ActionKind::Observe,
        }
    }

    fn criteria(self) -> &'static str {
        match self {
            Self::RollbackToPreviousVersion => {
                "The latest deployment caused the problem: the job started failing or misbehaving right after it, \
                 in a way its change could cause, and the previous version ran healthily. Rolling back restores \
                 the previous version from its last good state."
            }
            Self::RestartFromCheckpoint => {
                "The job is stuck or failed for a passing reason outside its code, state and input data, such as a \
                 consumer that stalled with no errors or a failure from lost infrastructure that it didn't recover \
                 from on its own. A clean restart from the last checkpoint is likely to clear it."
            }
            Self::ScaleUp => {
                "The job is healthy but can't keep up with its input: the input rate rose, lag keeps growing, and \
                 its tasks are busy and backpressured, without errors. More parallelism adds the capacity it needs."
            }
            Self::EscalateToOwner => {
                "Only the owning team can fix it, with a change to the job's code or configuration, or with its \
                 upstream producers: a bug that isn't from the latest deployment, state that grows without bound \
                 and needs a state TTL, or malformed or incompatible input data. A rollback, restart or more \
                 parallelism wouldn't fix it."
            }
            Self::EscalateToPlatform => {
                "The platform beneath the job is at fault, such as lost TaskManagers, storage or Kafka problems, \
                 often affecting several jobs. The platform team needs to fix it."
            }
            Self::Observe => {
                "Nothing needs doing yet: the symptoms are already easing on their own, and acting now would \
                 disrupt the job more than waiting a few minutes."
            }
        }
    }
}

/// Jev's pick, with its probability and the others'.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Decision {
    pub remediation: Remediation,
    pub probability: f64,
    pub probabilities: BTreeMap<String, f64>,
}

/// `remediation`: which fix the incident calls for. `state` is the decision
/// state (`facts::DecisionState`).
pub fn remediation_request(model: &str, state: Value) -> Request {
    let options: BTreeMap<String, Value> =
        Remediation::ALL.iter().map(|&r| (r.name().to_string(), Value::from(r.criteria()))).collect();
    Request::new(state).with_model(model).with_question(
        "remediation",
        Question::choice(
            json!({
                "question": "Which remediation does the incident on job `incident.job.name` call for?",
                "compare": ["`incident.triage`", "`evidence`", "`now`", "`latest_deploy`", "`previous_version`", "`capacity`"],
                "guidance": "`remediation_policy` says what each remediation can and can't fix on this platform.",
            }),
            options,
        ),
    )
}

pub fn decision(response: &Response) -> anyhow::Result<Decision> {
    let answer = response.answer("remediation").context("no answer to remediation")?;
    let choice = answer.choice().context("remediation isn't a choice")?;
    let remediation = Remediation::ALL
        .into_iter()
        .find(|r| r.name() == choice)
        .with_context(|| format!("unknown remediation {choice}"))?;
    let probabilities = answer.probabilities().cloned().unwrap_or_default();
    Ok(Decision { remediation, probability: probabilities.get(choice).copied().unwrap_or(0.0), probabilities })
}

/// `recovered`: is the job healthy again since the action? `state` is the
/// verification state (`facts::VerifyState`).
pub fn recovered_request(model: &str, state: Value) -> Request {
    Request::new(state).with_model(model).with_question(
        "recovered",
        Question::noul_with_criteria(
            json!({
                "question": "Has job `job` recovered since `action` was taken: is it running normally now, without \
                             the incident's symptoms?",
                "compare": ["`before`", "`after`"],
                "guidance": "`action.expected` says what recovery looks like for this action.",
            }),
            NoulCriteria::new(
                json!({
                    "what": "The job is RUNNING and has stayed up since the action, checkpoints complete, and the \
                             incident's symptoms are gone or clearly going.",
                    "examples": [
                        "no failures since a rollback, with checkpoints completing again",
                        "lag falling steadily and backpressure lower after a scale-up",
                    ],
                }),
                json!({
                    "what": "The symptoms persist or came back: the job keeps failing, isn't running, or lag and \
                             backpressure are as bad as before or worse.",
                    "examples": [
                        "the same exception again after the restart",
                        "lag still growing at the new parallelism",
                    ],
                }),
            ),
        ),
    )
}

pub fn recovered(response: &Response) -> anyhow::Result<f64> {
    response.answer("recovered").context("no answer to recovered")?.noul().context("recovered isn't a noul")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remediation_names_match_their_serde_names() {
        for r in Remediation::ALL {
            assert_eq!(serde_json::to_value(r).unwrap(), r.name());
        }
    }
}
