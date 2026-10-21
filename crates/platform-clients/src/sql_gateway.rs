//! A client for the Flink SQL Gateway's REST API, used to run SQL jobs.

use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQL Gateway request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("statement failed: {statement}: {cause}")]
    Statement { statement: String, cause: String },
    #[error("timed out waiting for statement: {0}")]
    Timeout(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct SqlGateway {
    base_url: String,
    http: reqwest::Client,
}

/// Splits a SQL script into statements: drops `--` comment lines and splits
/// on semicolons. (The job scripts don't put semicolons in strings.)
pub fn statements(script: &str) -> Vec<String> {
    let without_comments: String =
        script.lines().filter(|line| !line.trim_start().starts_with("--")).collect::<Vec<_>>().join("\n");
    without_comments
        .split(';')
        .map(|s| s.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|s| !s.is_empty())
        .collect()
}

impl SqlGateway {
    pub fn new(base_url: impl Into<String>) -> Self {
        let http = reqwest::Client::builder().timeout(Duration::from_secs(30)).build().expect("HTTP client");
        Self { base_url: base_url.into().trim_end_matches('/').to_string(), http }
    }

    /// Runs statements in a fresh session and returns the first result field
    /// of the last one (for an `INSERT`, the Flink job ID).
    pub async fn run(&self, session_name: &str, statements: &[String]) -> Result<Option<String>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Session {
            session_handle: String,
        }
        let session: Session = self
            .http
            .post(format!("{}/v1/sessions", self.base_url))
            .json(&serde_json::json!({"sessionName": session_name}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let mut last = None;
        let mut outcome = Ok(());
        for statement in statements {
            match self.execute(&session.session_handle, statement).await {
                Ok(result) => last = result,
                Err(err) => {
                    outcome = Err(err);
                    break;
                }
            }
        }
        let _ = self.http.delete(format!("{}/v1/sessions/{}", self.base_url, session.session_handle)).send().await;
        outcome.map(|()| last)
    }

    async fn execute(&self, session: &str, statement: &str) -> Result<Option<String>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Operation {
            operation_handle: String,
        }
        let short: String = statement.chars().take(80).collect();
        let operation: Operation = self
            .http
            .post(format!("{}/v1/sessions/{session}/statements", self.base_url))
            .json(&serde_json::json!({"statement": statement}))
            .send()
            .await?
            .json()
            .await
            .map_err(|_| Error::Statement { statement: short.clone(), cause: "rejected by the gateway".into() })?;
        let url = format!("{}/v1/sessions/{session}/operations/{}/result/0", self.base_url, operation.operation_handle);
        for _ in 0..240 {
            let result: serde_json::Value = self.http.get(&url).send().await?.json().await?;
            if let Some(errors) = result["errors"].as_array() {
                let cause = errors
                    .last()
                    .and_then(|e| e.as_str())
                    .map(|e| {
                        e.lines()
                            .filter(|l| l.contains("Exception") || l.starts_with("Caused by"))
                            .take(3)
                            .collect::<Vec<_>>()
                            .join(" | ")
                    })
                    .unwrap_or_default();
                return Err(Error::Statement { statement: short, cause });
            }
            if result["resultType"] != "NOT_READY" {
                return Ok(result["results"]["data"][0]["fields"][0].as_str().map(str::to_string));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(Error::Timeout(short))
    }
}

#[cfg(test)]
mod tests {
    use super::statements;

    #[test]
    fn splits_scripts_into_statements() {
        let script = "-- a comment; with a semicolon\nSET 'pipeline.name' = 'x';\n\nCREATE TABLE t (\n  a INT\n);\n-- trailing\nINSERT INTO t\nSELECT 1;\n";
        assert_eq!(
            statements(script),
            ["SET 'pipeline.name' = 'x'", "CREATE TABLE t ( a INT )", "INSERT INTO t SELECT 1"]
        );
    }
}
