//! The agent's tuning, from `triage.yaml`.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub model: String,
    /// Background for every judgment, put in the state as `triage_policy`.
    pub context: serde_json::Value,
    /// The known-noise list's path, relative to the config file.
    known_noise: PathBuf,
    /// The known-noise notes by job, loaded from `known_noise`.
    #[serde(skip)]
    pub notes: BTreeMap<String, KnownNoise>,
    #[serde(deserialize_with = "duration")]
    pub group_window: Duration,
    #[serde(deserialize_with = "duration")]
    pub recovery: Duration,
    #[serde(deserialize_with = "duration")]
    pub settle: Duration,
    #[serde(deserialize_with = "duration")]
    pub recheck: Duration,
    #[serde(deserialize_with = "duration")]
    pub history: Duration,
    #[serde(deserialize_with = "duration")]
    pub deploy_window: Duration,
    #[serde(deserialize_with = "duration")]
    pub incident_ttl: Duration,
    pub max_concurrent: usize,
    pub policy: Policy,
}

/// Thresholds that turn Jev's answers into a disposition (`policy.rs`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub raise: f64,
    pub impact_raise: f64,
    pub impact: f64,
    pub fix_class: f64,
    pub fix_class_raise: f64,
    pub transient_veto: f64,
    pub attach: f64,
}

/// The platform team's note on a job whose alerts are known noise.
#[derive(Debug, Clone, PartialEq, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnownNoise {
    #[serde(skip_serializing)]
    pub job: String,
    pub note: String,
    pub triaged_on: String,
    pub triaged_by: String,
}

fn duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let s = String::deserialize(d)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let mut config: Self = read_yaml(path)?;
        let notes_path = path.parent().unwrap_or(Path::new(".")).join(&config.known_noise);
        let notes: Vec<KnownNoise> = read_yaml(&notes_path)?;
        config.notes = notes.into_iter().map(|n| (n.job.clone(), n)).collect();
        Ok(config)
    }
}

fn read_yaml<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_yaml_ng::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_config_parses() {
        let config = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/triage.yaml"))).unwrap();
        assert_eq!(config.group_window, Duration::from_secs(45));
        assert!(config.policy.impact_raise < config.policy.raise);
        assert!(config.notes.len() > 100);
    }
}
