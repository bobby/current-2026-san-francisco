//! Prometheus's query API, for metric summaries the agents compute in code
//! (docs/design.md §6.1). Queries take an evaluation time, so evidence can be
//! gathered as of any moment, e.g. to replay a judgment.
//!
//! Flink's reporter rewrites `job_name` label values (`payments_enrichment`);
//! [`job_label`] turns a platform job name into that form.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Prometheus request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Prometheus rejected the query ({error_type}): {error}")]
    Query { error_type: String, error: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// One series of an instant query's result.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

/// One series of a range query's result.
#[derive(Debug, Clone, PartialEq)]
pub struct RangeSeries {
    pub labels: BTreeMap<String, String>,
    pub points: Vec<(DateTime<Utc>, f64)>,
}

#[derive(Debug, Clone)]
pub struct PrometheusClient {
    base_url: String,
    http: reqwest::Client,
}

/// The `job_name` label value Flink's reporter gives a job.
pub fn job_label(job_name: &str) -> String {
    job_name.chars().map(|c| if c.is_ascii_alphanumeric() || c == ':' || c == '_' { c } else { '_' }).collect()
}

#[derive(Deserialize)]
struct Envelope {
    status: String,
    data: Option<Data>,
    #[serde(rename = "errorType", default)]
    error_type: String,
    #[serde(default)]
    error: String,
}

#[derive(Deserialize)]
struct Data {
    result: Vec<VectorSample>,
}

#[derive(Deserialize)]
struct VectorSample {
    metric: BTreeMap<String, String>,
    value: (f64, String),
}

impl PrometheusClient {
    pub fn new(base_url: &str) -> Self {
        Self { base_url: base_url.trim_end_matches('/').to_string(), http: reqwest::Client::new() }
    }

    /// An instant query evaluated at `at`. Series whose value isn't a number
    /// (`NaN`) are left out.
    pub async fn query(&self, promql: &str, at: DateTime<Utc>) -> Result<Vec<Sample>> {
        let envelope: Envelope = self
            .http
            .get(format!("{}/api/v1/query", self.base_url))
            .query(&[("query", promql), ("time", &format!("{:.3}", at.timestamp_millis() as f64 / 1000.0))])
            .send()
            .await?
            .json()
            .await?;
        if envelope.status != "success" {
            return Err(Error::Query { error_type: envelope.error_type, error: envelope.error });
        }
        Ok(envelope
            .data
            .map(|d| d.result)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|s| {
                let value: f64 = s.value.1.parse().ok()?;
                value.is_finite().then_some(Sample { labels: s.metric, value })
            })
            .collect())
    }

    /// A range query: each series' values every `step` over `[from, to]`.
    pub async fn query_range(
        &self,
        promql: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        step: std::time::Duration,
    ) -> Result<Vec<RangeSeries>> {
        #[derive(Deserialize)]
        struct RangeEnvelope {
            status: String,
            data: Option<RangeData>,
            #[serde(rename = "errorType", default)]
            error_type: String,
            #[serde(default)]
            error: String,
        }
        #[derive(Deserialize)]
        struct RangeData {
            result: Vec<RangeSample>,
        }
        #[derive(Deserialize)]
        struct RangeSample {
            metric: BTreeMap<String, String>,
            values: Vec<(f64, String)>,
        }
        let seconds = |t: DateTime<Utc>| format!("{:.3}", t.timestamp_millis() as f64 / 1000.0);
        let envelope: RangeEnvelope = self
            .http
            .get(format!("{}/api/v1/query_range", self.base_url))
            .query(&[
                ("query", promql),
                ("start", &seconds(from)),
                ("end", &seconds(to)),
                ("step", &format!("{}s", step.as_secs().max(1))),
            ])
            .send()
            .await?
            .json()
            .await?;
        if envelope.status != "success" {
            return Err(Error::Query { error_type: envelope.error_type, error: envelope.error });
        }
        Ok(envelope
            .data
            .map(|d| d.result)
            .unwrap_or_default()
            .into_iter()
            .map(|s| RangeSeries {
                labels: s.metric,
                points: s
                    .values
                    .into_iter()
                    .filter_map(|(t, v)| {
                        let v: f64 = v.parse().ok()?;
                        let t = DateTime::from_timestamp_millis((t * 1000.0) as i64)?;
                        v.is_finite().then_some((t, v))
                    })
                    .collect(),
            })
            .collect())
    }

    /// The single value of a query that aggregates to one series, if any.
    pub async fn scalar(&self, promql: &str, at: DateTime<Utc>) -> Result<Option<f64>> {
        Ok(self.query(promql, at).await?.first().map(|s| s.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_labels_use_flinks_rewriting() {
        assert_eq!(job_label("payments-enrichment"), "payments_enrichment");
    }
}
