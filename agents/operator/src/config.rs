//! The agent's tuning, from `operator.yaml`.

use std::{collections::BTreeMap, path::Path, time::Duration};

use anyhow::Context;
use platform_model::{FailureClass, Tier};
use serde::{Deserialize, Deserializer};

use crate::questions::Remediation;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub model: String,
    /// Background for every decision, put in the state as
    /// `remediation_policy`.
    pub context: serde_json::Value,
    pub guardrails: Guardrails,
    #[serde(deserialize_with = "durations")]
    pub verify_after: BTreeMap<Remediation, Duration>,
    #[serde(deserialize_with = "duration")]
    pub verify_every: Duration,
    #[serde(deserialize_with = "duration")]
    pub verify_deadline: Duration,
    pub recovered: f64,
    #[serde(deserialize_with = "duration")]
    pub observe_for: Duration,
    pub observe_max: u32,
    #[serde(deserialize_with = "duration")]
    pub followup: Duration,
}

/// What the operator may do without a person (`guardrails.rs`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Guardrails {
    pub min_confidence: f64,
    pub min_class_confidence: f64,
    pub budget: usize,
    #[serde(deserialize_with = "duration")]
    pub budget_window: Duration,
    pub actions: Actions,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Actions {
    pub rollback_to_previous_version: RollbackRule,
    pub restart_from_checkpoint: Rule,
    pub scale_up: ScaleRule,
}

/// The failure classes and tiers an action is allowed for.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    pub classes: Vec<FailureClass>,
    pub tiers: Vec<Tier>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollbackRule {
    pub classes: Vec<FailureClass>,
    pub tiers: Vec<Tier>,
    #[serde(deserialize_with = "duration")]
    pub max_deploy_age: Duration,
    #[serde(deserialize_with = "duration")]
    pub symptoms_within: Duration,
    #[serde(deserialize_with = "duration")]
    pub previous_healthy_for: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScaleRule {
    pub classes: Vec<FailureClass>,
    pub tiers: Vec<Tier>,
    pub factor: u32,
}

fn duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let s = String::deserialize(d)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

fn durations<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<Remediation, Duration>, D::Error> {
    BTreeMap::<Remediation, String>::deserialize(d)?
        .into_iter()
        .map(|(k, v)| Ok((k, humantime::parse_duration(&v).map_err(serde::de::Error::custom)?)))
        .collect()
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_yaml_ng::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }

    /// How long after `remediation` to verify.
    pub fn verify_after(&self, remediation: Remediation) -> Duration {
        self.verify_after.get(&remediation).copied().unwrap_or(Duration::from_secs(120))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_config_parses() {
        let config = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/operator.yaml"))).unwrap();
        assert_eq!(config.guardrails.budget, 1);
        assert_eq!(config.verify_after(Remediation::RollbackToPreviousVersion), Duration::from_secs(90));
        assert!(config.guardrails.actions.rollback_to_previous_version.classes.contains(&FailureClass::CodeRegression));
    }
}
