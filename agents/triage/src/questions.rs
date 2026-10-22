//! The questions Jev answers about a group of alerts, in one request
//! (docs/design.md §8.1), and their answers as a typed judgment.
//!
//! All the questions see the same evidence and are answered independently.
//! `deploy_related` is asked only when a deployment is in the window, and
//! `same_incident` only when an incident is open for the job or its tenant.

use std::collections::BTreeMap;

use anyhow::Context;
use platform_model::FailureClass;
use serde::Serialize;
use serde_json::{Value, json};
use typesafe_ai::{NoulCriteria, Question, Request, Response};

use crate::evidence::Evidence;

/// The `same_incident` option for "a new problem".
pub const NEW_INCIDENT: &str = "none";

pub const CLASSES: [FailureClass; 8] = [
    FailureClass::CodeRegression,
    FailureClass::BadInputData,
    FailureClass::ResourceSaturation,
    FailureClass::StateGrowth,
    FailureClass::SchemaMismatch,
    FailureClass::PlatformInfrastructure,
    FailureClass::TransientOrExpected,
    FailureClass::Unknown,
];

pub fn class_name(class: FailureClass) -> &'static str {
    match class {
        FailureClass::CodeRegression => "code_regression",
        FailureClass::BadInputData => "bad_input_data",
        FailureClass::ResourceSaturation => "resource_saturation",
        FailureClass::StateGrowth => "state_growth",
        FailureClass::SchemaMismatch => "schema_mismatch",
        FailureClass::PlatformInfrastructure => "platform_infrastructure",
        FailureClass::TransientOrExpected => "transient_or_expected",
        FailureClass::Unknown => "unknown",
    }
}

fn class_criteria(class: FailureClass) -> &'static str {
    match class {
        FailureClass::CodeRegression => {
            "A bug in the job's own code, usually introduced by a recent deployment: the job fails with a \
             programming error (such as a NullPointerException) on ordinary input that the previous version handled."
        }
        FailureClass::BadInputData => {
            "Malformed or unexpected records arriving on the job's input: the job fails while parsing or \
             deserializing particular records, often from one producer, and the code itself is unchanged."
        }
        FailureClass::ResourceSaturation => {
            "The job can't keep up with its load: backpressure, busy tasks, lag growing and checkpoints slowing, \
             without errors, typically because input volume rose beyond the job's capacity."
        }
        FailureClass::StateGrowth => {
            "The job's state keeps growing without bound: checkpoint size and duration climb steadily over time, \
             toward checkpoint timeouts or memory exhaustion."
        }
        FailureClass::SchemaMismatch => {
            "The input's schema changed incompatibly with the job or its table: a field's type or shape no longer \
             matches, so records fail to convert or write."
        }
        FailureClass::PlatformInfrastructure => {
            "A problem in the platform beneath the job rather than the job: lost TaskManagers, network or storage \
             errors, or Kafka being unavailable, often affecting several jobs."
        }
        FailureClass::TransientOrExpected => {
            "Nothing needs fixing: an expected restart right after a deploy, a single blip that recovered, or \
             symptoms that match the platform team's `known_noise` note for the job, such as a recurring benign \
             failure it always recovers from or a lag peak at its tenant's busy time."
        }
        FailureClass::Unknown => "The evidence doesn't point to any of the other classes.",
    }
}

/// The impact levels, in order.
const IMPACT: [&str; 4] = [
    "No impact: the job is running and keeping up, and its output is current.",
    "Slight delay: the job is processing and catching up, but its output arrives somewhat late for a while.",
    "Stale data: the job is falling further behind or keeps failing, so its output is increasingly late or \
     missing.",
    "Stopped or data at risk: the job has stopped producing output, is stuck in a restart loop, or is losing or \
     corrupting data.",
];

pub fn request(model: &str, evidence: &Evidence) -> anyhow::Result<Request> {
    let mut request = Request::new(serde_json::to_value(evidence)?)
        .with_model(model)
        .with_question(
            "actionable",
            Question::noul_with_criteria(
                json!({
                    "question": "Do these alerts reflect a genuine, persistent problem with job `job.name` that an \
                                 operator needs to act on?",
                    "compare": ["`alerts`", "`status`", "`failures`", "`checkpoints`", "`metrics`", "`known_noise`"],
                    "guidance": "`triage_policy` says what this platform's alerts usually mean.",
                }),
                NoulCriteria::new(
                    json!({
                        "what": "A problem that persists or keeps getting worse, of a kind `triage_policy.needs_action` \
                                 lists.",
                        "examples": [
                            "the same exception over and over, the job never staying up",
                            "lag growing for many minutes while input keeps arriving",
                            "state size and checkpoint durations climbing steadily",
                        ],
                    }),
                    json!({
                        "what": "A transient or expected condition, of a kind `triage_policy.usually_noise` lists.",
                        "not_for": "A failure that keeps repeating, even when its cause is outside the job.",
                        "examples": [
                            "one restart after a deploy, then running normally",
                            "one TaskManager loss the job recovered from",
                            "a slow checkpoint followed by normal ones",
                            "symptoms matching the `known_noise` note",
                        ],
                    }),
                ),
            ),
        )
        .with_question(
            "failure_class",
            Question::choice(
                "What kind of problem do these alerts on job `job.name` point to, given its `failures`, \
                 `log_messages`, `checkpoints`, `metrics` and `deployments`?",
                CLASSES.iter().map(|&c| (class_name(c).to_string(), Value::from(class_criteria(c)))).collect(),
            ),
        )
        .with_question(
            "user_impact",
            Question::score(
                "How badly are the consumers of job `job.name`'s output affected right now? Judge from the job's \
                 current state: whether `status.current` is RUNNING, how `metrics` lag and input rate compare with \
                 before, and whether checkpoints are still completing.",
                IMPACT.iter().map(|&l| Value::from(l)).collect(),
            ),
        );
    if !evidence.deployments.is_empty() {
        request.insert_question(
            "deploy_related",
            Question::noul_with_criteria(
                "Did the most recent deployment, `deployments[0]`, probably cause these symptoms on job `job.name`?",
                NoulCriteria::new(
                    "The symptoms began soon after the deployment and fit its change: new errors in what the change \
                     touches, where the job was healthy before.",
                    "The symptoms have another explanation: they started well after the deployment or before it, the \
                     change can't plausibly cause them, or the evidence points to input data, load or the platform.",
                ),
            ),
        );
    }
    if !evidence.open_incidents.is_empty() {
        let mut options: BTreeMap<String, Value> = evidence
            .open_incidents
            .iter()
            .map(|i| {
                (
                    i.id.clone(),
                    json!({
                        "the alerts are part of this open incident": {
                            "job": i.job,
                            "failure_class": i.failure_class,
                            "summary": i.summary,
                        }
                    }),
                )
            })
            .collect();
        options.insert(
            NEW_INCIDENT.to_string(),
            Value::from("The alerts are a separate problem from every open incident."),
        );
        request.insert_question(
            "same_incident",
            Question::choice(
                "Are these alerts on job `job.name` part of one of the `open_incidents`, the same underlying \
                 problem showing more symptoms, or a new problem?",
                options,
            ),
        );
    }
    Ok(request)
}

/// Jev's answers, typed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Judgment {
    pub actionable: f64,
    pub failure_class: FailureClass,
    pub class_probabilities: BTreeMap<String, f64>,
    /// From 0 (no impact) to 3 (stopped or data at risk).
    pub user_impact: f64,
    pub deploy_related: Option<f64>,
    /// The chosen option and its probability, when asked.
    pub same_incident: Option<(String, f64)>,
}

impl Judgment {
    pub fn class_probability(&self, class: FailureClass) -> f64 {
        self.class_probabilities.get(class_name(class)).copied().unwrap_or(0.0)
    }
}

pub fn judgment(response: &Response) -> anyhow::Result<Judgment> {
    let answer = |id: &str| response.answer(id).with_context(|| format!("no answer to {id}"));
    let class = answer("failure_class")?;
    let choice = class.choice().context("failure_class isn't a choice")?;
    let failure_class = CLASSES
        .into_iter()
        .find(|&c| class_name(c) == choice)
        .with_context(|| format!("unknown failure class {choice}"))?;
    let same_incident = match response.answer("same_incident") {
        Some(a) => {
            let choice = a.choice().context("same_incident isn't a choice")?.to_string();
            let p = a.probabilities().and_then(|p| p.get(&choice)).copied().unwrap_or(0.0);
            Some((choice, p))
        }
        None => None,
    };
    Ok(Judgment {
        actionable: answer("actionable")?.noul().context("actionable isn't a noul")?,
        failure_class,
        class_probabilities: class.probabilities().cloned().unwrap_or_default(),
        user_impact: answer("user_impact")?.score().context("user_impact isn't a score")?,
        deploy_related: response.answer("deploy_related").and_then(|a| a.noul()),
        same_incident,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_names_match_the_models_serde_names() {
        for c in CLASSES {
            assert_eq!(serde_json::to_value(c).unwrap(), class_name(c));
        }
    }
}
