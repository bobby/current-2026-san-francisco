//! The Claude Code bridge (Scenario 2, docs/design.md §6.2, §8.2). A post
//! that mentions `@ops-assistant` starts, or continues, a headless Claude
//! Code session for its thread:
//!
//! - one session per thread (`--session-id` the first time, `--resume`
//!   after), so follow-up questions keep their context;
//! - Claude can use only the platform's MCP server (`mcp-server`, started
//!   over stdio with the thread's context in its environment), whose tools
//!   are pre-approved; every built-in tool is off and anything else is
//!   refused;
//! - each tool call becomes a short status line in the thread, and Claude's
//!   answer the final reply.

use std::{collections::HashMap, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use anyhow::Context as _;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::{Mutex, Semaphore},
};
use tracing::{info, warn};
use uuid::Uuid;

/// The platform's system prompt, appended to Claude Code's own.
const SYSTEM_PROMPT: &str = include_str!("assistant-prompt.md");

/// The MCP server's tools, all pre-approved: they read, or only propose.
pub const TOOLS: [&str; 10] = [
    "get_signal",
    "get_job",
    "get_deployments",
    "query_prometheus",
    "get_logs",
    "get_checkpoints",
    "sample_topic",
    "get_ticket",
    "search_tickets",
    "propose_remediation",
];

#[derive(Debug, Clone)]
pub struct AssistantConfig {
    pub claude: String,
    pub model: String,
    /// Claude Code's home: its sessions live here.
    pub home: PathBuf,
    /// The MCP server's environment: the services it reads.
    pub mcp_env: HashMap<String, String>,
    pub timeout: Duration,
}

/// Where a conversation happens, and what it's about.
#[derive(Debug, Clone)]
pub struct Context {
    pub root_id: String,
    pub channel: &'static str,
    pub signal_id: Option<Uuid>,
    /// What the thread's first post says, for a new session.
    pub about: String,
}

/// What a run produced, as it happens.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Claude called a tool: a status line for the thread.
    Status(String),
    /// Claude's answer.
    Answer(String),
    Failed(String),
}

pub struct Assistant {
    config: AssistantConfig,
    /// Thread root → its session, one run at a time per thread.
    sessions: Mutex<HashMap<String, Arc<Mutex<Session>>>>,
    /// Claude Code runs at once, across threads.
    runs: Semaphore,
}

#[derive(Debug)]
struct Session {
    id: Uuid,
    started: bool,
}

impl Assistant {
    pub fn new(config: AssistantConfig) -> Self {
        Self { config, sessions: Mutex::new(HashMap::new()), runs: Semaphore::new(2) }
    }

    /// Forgets every thread's session (the director's reset).
    pub async fn forget(&self) {
        self.sessions.lock().await.clear();
    }

    /// Asks Claude `question` in the thread's session, reporting events on
    /// `events` as they happen.
    pub async fn ask(
        &self,
        context: &Context,
        question: &str,
        events: tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> anyhow::Result<()> {
        let session = self
            .sessions
            .lock()
            .await
            .entry(context.root_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(Session { id: Uuid::new_v4(), started: false })))
            .clone();
        let mut session = session.lock().await;
        let _permit = self.runs.acquire().await?;
        let prompt = if session.started {
            question.to_string()
        } else {
            format!("{}\n\nThe engineer asks: {question}", context.about)
        };
        let mut command = self.command(context, &session, &prompt);
        let mut child = command.spawn().context("starting Claude Code")?;
        session.started = true;
        let stdout = child.stdout.take().context("Claude Code's stdout")?;
        let mut lines = BufReader::new(stdout).lines();
        let read = async {
            let mut answered = false;
            while let Some(line) = lines.next_line().await? {
                for event in events_of(&line) {
                    answered |= !matches!(event, Event::Status(_));
                    let _ = events.send(event);
                }
            }
            anyhow::Ok(answered)
        };
        let answered = match tokio::time::timeout(self.config.timeout, read).await {
            Ok(result) => result?,
            Err(_) => {
                let _ = child.kill().await;
                let _ = events.send(Event::Failed(format!(
                    "I ran out of time ({} s) before finishing.",
                    self.config.timeout.as_secs()
                )));
                return Ok(());
            }
        };
        let status = child.wait().await?;
        if !answered {
            warn!(?status, "Claude Code ended without an answer");
            let _ = events.send(Event::Failed(format!("Claude Code ended without an answer ({status}).")));
        }
        Ok(())
    }

    fn command(&self, context: &Context, session: &Session, prompt: &str) -> Command {
        let mut env = self.config.mcp_env.clone();
        env.insert("THREAD_ROOT_ID".into(), context.root_id.clone());
        env.insert("THREAD_CHANNEL".into(), context.channel.into());
        if let Some(id) = context.signal_id {
            env.insert("SIGNAL_ID".into(), id.to_string());
        }
        let mcp =
            json!({"mcpServers": {"platform": {"type": "stdio", "command": "mcp-server", "args": [], "env": env}}});
        let allowed: Vec<String> = TOOLS.iter().map(|t| format!("mcp__platform__{t}")).collect();
        let mut command = Command::new(&self.config.claude);
        command
            .arg("--print")
            .arg(prompt)
            .args(["--output-format", "stream-json", "--verbose"])
            .args(["--model", &self.config.model])
            // No built-in tools (shell, files, web): only the platform's.
            .args(["--tools", ""])
            .args(["--mcp-config", &mcp.to_string(), "--strict-mcp-config"])
            .arg("--allowedTools")
            .arg(allowed.join(","))
            // Anything not pre-approved is refused, not asked about.
            .args(["--permission-mode", "dontAsk"])
            .args(["--append-system-prompt", SYSTEM_PROMPT]);
        if session.started {
            command.args(["--resume", &session.id.to_string()]);
        } else {
            command.args(["--session-id", &session.id.to_string()]);
        }
        command
            .current_dir(&self.config.home)
            .env("HOME", &self.config.home)
            .env("DISABLE_AUTOUPDATER", "1")
            .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        info!(root_id = context.root_id, session = %session.id, resume = session.started, "asking Claude");
        command
    }
}

/// The thread-worthy events in one line of Claude Code's `stream-json`
/// output: tool calls and the final result.
pub fn events_of(line: &str) -> Vec<Event> {
    let Ok(event) = serde_json::from_str::<Value>(line) else { return vec![] };
    match event["type"].as_str() {
        Some("assistant") => event["message"]["content"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|block| block["type"] == "tool_use")
            .map(|block| Event::Status(status_line(block["name"].as_str().unwrap_or_default(), &block["input"])))
            .collect(),
        Some("result") => {
            let text = event["result"].as_str().unwrap_or_default().trim().to_string();
            if event["is_error"].as_bool().unwrap_or(false) || text.is_empty() {
                let why = if text.is_empty() { event["subtype"].as_str().unwrap_or("error").to_string() } else { text };
                vec![Event::Failed(why)]
            } else {
                vec![Event::Answer(text)]
            }
        }
        _ => vec![],
    }
}

/// A short status line for a tool call, e.g. "🔎 checking deployment
/// history for `trade-events-ingest`…".
pub fn status_line(tool: &str, input: &Value) -> String {
    let s = |key: &str| input[key].as_str().map(|v| format!("`{v}`"));
    let job = s("job").unwrap_or_else(|| "the job".into());
    match tool.strip_prefix("mcp__platform__").unwrap_or(tool) {
        "get_signal" => "🧾 reading the triage signal…".into(),
        "get_job" => format!("🔎 checking {job}'s status…"),
        "get_deployments" => format!("🔎 checking deployment history for {job}…"),
        "query_prometheus" => {
            let promql = input["promql"].as_str().unwrap_or_default();
            let metric = promql
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .find(|w| w.starts_with("flink_"))
                .map(|m| m.rsplit('_').next().unwrap_or(m).to_string());
            match metric {
                Some(m) => format!("📈 querying Prometheus for `{m}`…"),
                None => "📈 querying Prometheus…".into(),
            }
        }
        "get_logs" => format!("📜 reading {job}'s logs…"),
        "get_checkpoints" => format!("💾 checking {job}'s checkpoints…"),
        "sample_topic" => {
            let topic = s("topic").unwrap_or_else(|| "a topic".into());
            match (input["partition"].as_i64(), input["offset"].as_i64()) {
                (Some(p), Some(o)) => format!("🧪 sampling {topic} partition {p} from offset {o}…"),
                _ => format!("🧪 sampling records on {topic}…"),
            }
        }
        "get_ticket" => format!("🎫 looking up {}…", s("number").unwrap_or_else(|| "a ticket".into())),
        "search_tickets" => "🎫 searching tickets…".into(),
        "propose_remediation" => "📝 proposing a remediation for approval…".into(),
        other => format!("🔧 {other}…"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_calls_become_status_lines_and_the_result_the_answer() {
        let call = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"Let me check."},
            {"type":"tool_use","name":"mcp__platform__get_deployments","input":{"job":"trade-events-ingest"}}]}}"#;
        assert_eq!(
            events_of(call),
            [Event::Status("🔎 checking deployment history for `trade-events-ingest`…".into())]
        );
        let sample = json!({"topic": "trade-events", "partition": 2, "offset": 4417});
        assert_eq!(
            status_line("mcp__platform__sample_topic", &sample),
            "🧪 sampling `trade-events` partition 2 from offset 4417…"
        );
        let query = json!({"promql": "sum(flink_taskmanager_job_task_operator_pendingRecords{job_name=\"x\"})"});
        assert_eq!(
            status_line("mcp__platform__query_prometheus", &query),
            "📈 querying Prometheus for `pendingRecords`…"
        );
        let done = r#"{"type":"result","subtype":"success","is_error":false,"result":"It's the records."}"#;
        assert_eq!(events_of(done), [Event::Answer("It's the records.".into())]);
        let failed = r#"{"type":"result","subtype":"error_max_turns","is_error":true,"result":""}"#;
        assert_eq!(events_of(failed), [Event::Failed("error_max_turns".into())]);
        assert!(events_of("not json").is_empty());
    }

    #[test]
    fn the_prompt_names_every_tool_it_relies_on() {
        assert!(SYSTEM_PROMPT.contains("propose_remediation"));
        assert!(TOOLS.contains(&"propose_remediation"));
    }
}
