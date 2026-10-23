//! cicd-sim's API (docs/design.md §4.3): the version catalogue, deploys and
//! rollbacks. Every deploy goes through the same pipeline and emits a
//! `platform.deployments` event; a request with an `action_id` also gets its
//! outcome recorded in `platform.actions`.

use std::collections::BTreeMap;

use platform_model::{Deployment, JobKind, Tier};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cicd-sim request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cicd-sim rejected the request ({status}): {body}")]
    Rejected { status: u16, body: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// The jobs the platform's pipeline deploys (`jobs/versions.yaml`).
#[derive(Debug, Clone, Deserialize)]
pub struct Catalogue {
    pub jobs: BTreeMap<String, CatalogueJob>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CatalogueJob {
    pub tenant: String,
    pub owner: String,
    pub tier: Tier,
    pub kind: JobKind,
    /// The most parallelism the tenant's policy allows.
    pub max_parallelism: u32,
    pub baseline: String,
    pub versions: Vec<CatalogueVersion>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CatalogueVersion {
    pub version: String,
    pub commit_sha: String,
    pub author: String,
    pub summary: String,
}

/// Who asks, and the remediation the deploy carries out.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RequestContext {
    pub triggered_by: String,
    pub action_id: Option<Uuid>,
    pub incident_id: Option<Uuid>,
    pub signal_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct CicdClient {
    base_url: String,
    http: reqwest::Client,
}

impl CicdClient {
    pub fn new(base_url: &str) -> Self {
        // A deploy waits for a savepoint and for Flink to start the new run.
        let http =
            reqwest::Client::builder().timeout(std::time::Duration::from_secs(180)).build().expect("HTTP client");
        Self { base_url: base_url.trim_end_matches('/').to_string(), http }
    }

    /// A deploy that cicd-sim ran but that failed comes back as a
    /// `Deployment` with outcome `failed`, not as an error.
    async fn send<T: serde::de::DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> Result<T> {
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() && status != reqwest::StatusCode::BAD_GATEWAY {
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Rejected { status: status.as_u16(), body });
        }
        Ok(response.json().await?)
    }

    pub async fn catalogue(&self) -> Result<Catalogue> {
        self.send(self.http.get(format!("{}/catalogue", self.base_url))).await
    }

    /// Deploys `version`, adding `args` to the version's own (Java jobs).
    pub async fn deploy(
        &self,
        job: &str,
        version: &str,
        args: &[String],
        context: &RequestContext,
    ) -> Result<Deployment> {
        #[derive(Serialize)]
        struct Body<'a> {
            job: &'a str,
            version: &'a str,
            args: &'a [String],
            #[serde(flatten)]
            context: &'a RequestContext,
        }
        let body = Body { job, version, args, context };
        self.send(self.http.post(format!("{}/deploy", self.base_url)).json(&body)).await
    }

    /// Rolls back to `to_version`, or to the version before the current one.
    pub async fn rollback(&self, job: &str, to_version: Option<&str>, context: &RequestContext) -> Result<Deployment> {
        #[derive(Serialize)]
        struct Body<'a> {
            job: &'a str,
            to_version: Option<&'a str>,
            #[serde(flatten)]
            context: &'a RequestContext,
        }
        let body = Body { job, to_version, context };
        self.send(self.http.post(format!("{}/rollback", self.base_url)).json(&body)).await
    }
}
