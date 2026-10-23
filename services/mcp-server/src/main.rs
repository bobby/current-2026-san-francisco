//! The platform's MCP server (Scenario 2, docs/design.md §6.2, §8.2): the
//! tools Claude Code investigates with.
//!
//! Every tool reads, except `propose_remediation`, which only asks the
//! chatops-bot to post an approval card: a person decides, and nothing
//! changes until they approve. The server reads the ops DB as a read-only
//! role.
//!
//! The chatops-bot's Claude bridge starts Claude Code once per message, and
//! Claude Code starts this server over stdio. The bridge passes the
//! conversation's context in the environment: the Mattermost thread
//! (`THREAD_ROOT_ID`, `THREAD_CHANNEL`) and, when the thread is about a
//! triage signal, `SIGNAL_ID`.

mod tools;

use anyhow::Context;
use clap::Parser;
use rmcp::{ServiceExt, transport::stdio};
use tools::{Platform, Thread};

#[derive(Debug, Parser)]
#[command(about = "The platform's MCP server for Claude Code (stdio)")]
struct Config {
    #[arg(long, env = "OPS_DATABASE_URL", default_value = "postgres://assistant:assistant@postgres:5432/ops")]
    ops_database_url: String,
    #[arg(long, env = "PROMETHEUS_URL", default_value = "http://prometheus:9090")]
    prometheus_url: String,
    #[arg(long, env = "FLINK_URL", default_value = "http://jobmanager:8081")]
    flink_url: String,
    #[arg(long, env = "CICD_URL", default_value = "http://cicd-sim:8091")]
    cicd_url: String,
    #[arg(long, env = "TICKETS_URL", default_value = "http://tickets:8094")]
    tickets_url: String,
    #[arg(long, env = "CHATOPS_URL", default_value = "http://localhost:8092")]
    chatops_url: String,
    #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS", default_value = "kafka:9092")]
    kafka_bootstrap_servers: String,
    /// The Mattermost thread the conversation is in.
    #[arg(long, env = "THREAD_ROOT_ID")]
    thread_root_id: Option<String>,
    #[arg(long, env = "THREAD_CHANNEL")]
    thread_channel: Option<String>,
    /// The triage signal the thread is about, if any.
    #[arg(long, env = "SIGNAL_ID")]
    signal_id: Option<uuid::Uuid>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // stdout carries the protocol; logs go to stderr.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .with_writer(std::io::stderr)
        .init();
    let config = Config::parse();
    let platform = Platform::connect(
        &config.ops_database_url,
        &config.prometheus_url,
        &config.flink_url,
        &config.cicd_url,
        &config.tickets_url,
        &config.chatops_url,
        &config.kafka_bootstrap_servers,
        Thread { root_id: config.thread_root_id, channel: config.thread_channel, signal_id: config.signal_id },
    )
    .await?;
    let service = platform.serve(stdio()).await.context("starting the MCP server")?;
    service.waiting().await?;
    Ok(())
}
