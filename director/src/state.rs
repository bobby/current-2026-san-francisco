//! The director's record of the latest reset and run, in
//! `.director/state.json` under the platform directory, so `status` can
//! report from another shell.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    pub reset_at: Option<DateTime<Utc>>,
    pub run: Option<Run>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub scenario: String,
    pub seed: u64,
    pub started_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub outcome: Outcome,
    pub steps: Vec<StepRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Running,
    Finished,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    /// Seconds from the start of the run.
    pub at_secs: u64,
    pub scenario: u8,
    pub action: String,
    pub note: String,
    pub status: StepStatus,
    pub done_at: Option<DateTime<Utc>>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Done,
    Failed,
}

fn path(dir: &Path) -> PathBuf {
    dir.join(".director").join("state.json")
}

impl State {
    pub fn load(dir: &Path) -> Self {
        std::fs::read_to_string(path(dir)).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
    }

    pub fn save(&self, dir: &Path) -> anyhow::Result<()> {
        let path = path(dir);
        std::fs::create_dir_all(path.parent().expect("state directory"))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}
