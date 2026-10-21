//! The version catalogue (`jobs/versions.yaml`).

use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, bail};
use platform_model::{JobKind, Tier};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalogue {
    pub jobs: BTreeMap<String, JobSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    pub tenant: String,
    pub owner: String,
    pub tier: Tier,
    pub kind: JobKind,
    pub max_parallelism: u32,
    /// The version the job runs before any scenario starts.
    pub baseline: String,
    pub versions: Vec<Version>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    pub version: String,
    pub commit_sha: String,
    pub author: String,
    pub summary: String,
    /// Job arguments (Java jobs), e.g. a fault flag.
    #[serde(default)]
    pub args: Vec<String>,
}

impl Catalogue {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let catalogue: Self = serde_yaml_ng::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        catalogue.validate()?;
        Ok(catalogue)
    }

    fn validate(&self) -> anyhow::Result<()> {
        for (name, job) in &self.jobs {
            let mut seen = std::collections::HashSet::new();
            for v in &job.versions {
                if !seen.insert(&v.version) {
                    bail!("{name}: version {} is listed twice", v.version);
                }
                if job.kind == JobKind::Sql && !v.args.is_empty() {
                    bail!("{name} {}: SQL jobs take no args", v.version);
                }
            }
            if job.version(&job.baseline).is_none() {
                bail!("{name}: baseline {} isn't a listed version", job.baseline);
            }
        }
        Ok(())
    }

    pub fn job(&self, name: &str) -> Option<&JobSpec> {
        self.jobs.get(name)
    }
}

impl JobSpec {
    pub fn version(&self, version: &str) -> Option<&Version> {
        self.versions.iter().find(|v| v.version == version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_repo_catalogue_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../jobs/versions.yaml");
        let catalogue = Catalogue::load(&path).unwrap();
        let payments = catalogue.job("payments-enrichment").unwrap();
        assert_eq!(payments.kind, JobKind::Java);
        assert_eq!(payments.version("1.42.0").unwrap().args, ["--fault=npe-on-null-field"]);
        assert_eq!(catalogue.jobs.len(), 6);
    }

    #[test]
    fn rejects_an_unknown_baseline() {
        let catalogue: Catalogue = serde_yaml_ng::from_str(
            "jobs:\n  j:\n    tenant: t\n    owner: o\n    tier: tier1\n    kind: sql\n    max_parallelism: 1\n    baseline: 2.0.0\n    versions:\n      - {version: 1.0.0, commit_sha: a, author: b, summary: c}\n",
        )
        .unwrap();
        assert!(catalogue.validate().is_err());
    }
}
