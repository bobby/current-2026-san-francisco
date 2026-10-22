//! The demo timeline (`director/timeline.yaml`): deploys and fault
//! injections at fixed offsets, each tagged with the scenario it belongs to.

use std::{collections::BTreeMap, path::Path, time::Duration};

use anyhow::Context;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Deserialize)]
pub struct Timeline {
    /// Seeds every ShadowTraffic run: the inputs, the fleet and the fault
    /// injectors.
    pub seed: u64,
    pub scenarios: BTreeMap<u8, Scenario>,
    pub steps: Vec<Step>,
    /// When a full run (`--scenario all`) ends.
    #[serde(deserialize_with = "duration")]
    pub end: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Scenario {
    pub title: String,
    /// How long a run of this scenario alone continues after its last step.
    #[serde(deserialize_with = "duration")]
    pub settle: Duration,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Step {
    #[serde(deserialize_with = "duration")]
    pub at: Duration,
    pub scenario: u8,
    pub note: String,
    #[serde(flatten)]
    pub action: Action,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// A versioned deploy through cicd-sim.
    Deploy {
        job: String,
        version: String,
        #[serde(default)]
        args: Vec<String>,
    },
    /// Starts a fault injector, the Compose service `st-fault-<name>`.
    Fault(String),
}

impl Action {
    pub fn describe(&self) -> String {
        match self {
            Self::Deploy { job, version, args } if args.is_empty() => format!("deploy {job} {version}"),
            Self::Deploy { job, version, args } => format!("deploy {job} {version} {}", args.join(" ")),
            Self::Fault(name) => format!("inject {name}"),
        }
    }
}

/// Which part of the timeline to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    All,
    Scenario(u8),
}

impl std::str::FromStr for Selection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(Self::All),
            n => n.parse().map(Self::Scenario).map_err(|_| format!("expected 1, 2, 3 or all, not {n:?}")),
        }
    }
}

impl std::fmt::Display for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => f.write_str("all"),
            Self::Scenario(n) => write!(f, "{n}"),
        }
    }
}

/// The steps to run, with offsets from the start of the run, and when the
/// run ends.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub steps: Vec<Step>,
    pub end: Duration,
}

impl Timeline {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let timeline: Self = serde_yaml_ng::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        timeline.validate()?;
        Ok(timeline)
    }

    fn validate(&self) -> anyhow::Result<()> {
        for step in &self.steps {
            anyhow::ensure!(
                self.scenarios.contains_key(&step.scenario),
                "step at {:?} names scenario {}, which isn't defined",
                step.at,
                step.scenario
            );
            anyhow::ensure!(step.at <= self.end, "step at {:?} is after the timeline's end", step.at);
            if let Action::Fault(name) = &step.action {
                anyhow::ensure!(
                    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric()),
                    "fault names are like f2, not {name:?}"
                );
            }
        }
        Ok(())
    }

    /// The whole timeline, or one scenario's steps on their own: shifted so
    /// the first starts at once, with the gaps between them kept, and the
    /// scenario's settle time after the last.
    pub fn plan(&self, selection: Selection) -> anyhow::Result<Plan> {
        let mut steps = self.steps.clone();
        steps.sort_by_key(|s| s.at);
        match selection {
            Selection::All => Ok(Plan { steps, end: self.end }),
            Selection::Scenario(n) => {
                let scenario = self.scenarios.get(&n).with_context(|| format!("no scenario {n} in the timeline"))?;
                let mut steps: Vec<Step> = steps.into_iter().filter(|s| s.scenario == n).collect();
                let shift = steps.first().map(|s| s.at).unwrap_or_default();
                for step in &mut steps {
                    step.at -= shift;
                }
                let end = steps.last().map(|s| s.at).unwrap_or_default() + scenario.settle;
                Ok(Plan { steps, end })
            }
        }
    }
}

fn duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    humantime::parse_duration(&text).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TIMELINE: &str = r#"
seed: 42
end: 30m
scenarios:
  1: {title: Noise, settle: 8m}
  2: {title: F2, settle: 10m}
  3: {title: F1 F3 F4, settle: 12m}
steps:
  - {at: 11m, scenario: 2, note: poison, fault: f2}
  - {at: 0s, scenario: 2, note: red herring, deploy: {job: trade-events-ingest, version: 2.18.0}}
  - {at: 1m, scenario: 3, note: F4, deploy: {job: positions-aggregator, version: 0.9.0, args: [--state-leak-bytes=8192]}}
  - {at: 15m, scenario: 3, note: F1, deploy: {job: payments-enrichment, version: 1.42.0}}
"#;

    fn timeline() -> Timeline {
        let t: Timeline = serde_yaml_ng::from_str(TIMELINE).unwrap();
        t.validate().unwrap();
        t
    }

    #[test]
    fn a_full_run_keeps_every_offset_in_order() {
        let plan = timeline().plan(Selection::All).unwrap();
        let at: Vec<u64> = plan.steps.iter().map(|s| s.at.as_secs() / 60).collect();
        assert_eq!(at, [0, 1, 11, 15]);
        assert_eq!(plan.end, Duration::from_secs(30 * 60));
        assert_eq!(plan.steps[2].action, Action::Fault("f2".into()));
        assert_eq!(plan.steps[1].action.describe(), "deploy positions-aggregator 0.9.0 --state-leak-bytes=8192");
    }

    #[test]
    fn a_scenario_alone_starts_at_once_and_keeps_its_gaps() {
        let plan = timeline().plan(Selection::Scenario(3)).unwrap();
        let at: Vec<u64> = plan.steps.iter().map(|s| s.at.as_secs() / 60).collect();
        assert_eq!(at, [0, 14]);
        assert_eq!(plan.end, Duration::from_secs((14 + 12) * 60));

        let noise_only = timeline().plan(Selection::Scenario(1)).unwrap();
        assert!(noise_only.steps.is_empty());
        assert_eq!(noise_only.end, Duration::from_secs(8 * 60));
        assert!(timeline().plan(Selection::Scenario(9)).is_err());
    }

    #[test]
    fn rejects_steps_for_unknown_scenarios() {
        let mut t = timeline();
        t.steps[0].scenario = 7;
        assert!(t.validate().is_err());
    }
}
