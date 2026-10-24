//! The chatops-bot (docs/design.md §9.1): the platform's only Mattermost
//! client.
//!
//! - On start it sets up the workspace: the admin, the `platform` team, the
//!   channels and the bot accounts (`setup`).
//! - It posts every `platform.alerts` event to `#platform-alerts` (the raw
//!   firehose).
//! - Agents post typed cards through `POST /api/posts` and update them with
//!   `PUT /api/posts/{post_id}`; the templates are in `cards`.
//! - Mattermost calls `POST /actions` when someone clicks a card's button;
//!   the decision goes to `platform.actions`.
//! - The tickets service calls `POST /hooks/tickets` after every change; the
//!   ticket's card follows.
//! - It listens on Mattermost's WebSocket and answers posts that mention a
//!   ticket number with its card.
//! - A post that mentions `@ops-assistant` goes to Claude Code, in a session
//!   per thread (`assistant`). Claude investigates through the platform's MCP
//!   server and can propose a remediation (`POST /api/proposals`): an
//!   approval card whose **Approve** the bot carries out (`remediation`).
//! - `POST /admin/reset-channels` archives and re-creates the channels (the
//!   director's reset).
//! - It keeps a transcript of every post and edit in the platform channels
//!   since the last reset (`GET /admin/transcript`), which `director record`
//!   saves with a run. `director replay` posts it again through
//!   `POST /admin/replay/posts` and `PUT /admin/replay/posts/{post_id}`
//!   (docs/design.md §10.3).

mod assistant;
mod bot;
mod cards;
mod mattermost;
mod remediation;
mod setup;

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use bot::{ActionRequest, Chatops};
use clap::Parser;
use futures_util::StreamExt;
use platform_clients::{
    chatops::{PostRequest, ProposalRequest, ReplayEdit, ReplayPost},
    cicd::CicdClient,
    flink::FlinkClient,
    kafka::Publisher,
    tickets::TicketNotification,
};
use platform_model::{AlertEvent, topics};
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};
use serde_json::json;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};
use tracing::{info, warn};

#[derive(Debug, Clone, Parser)]
#[command(about = "The platform's Mattermost bot: cards, buttons, the alert firehose and ticket updates")]
struct Config {
    #[arg(long, env = "MATTERMOST_URL", default_value = "http://mattermost:8065")]
    mattermost_url: String,
    /// The on-call engineer people log in as; created on a new server.
    #[arg(long, env = "MATTERMOST_ADMIN_USER", default_value = "oncall")]
    admin_user: String,
    #[arg(long, env = "MATTERMOST_ADMIN_PASSWORD", default_value = "oncall-demo-2026")]
    admin_password: String,
    #[arg(long, env = "MATTERMOST_TEAM", default_value = "platform")]
    team: String,
    #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS", default_value = "kafka:9092")]
    kafka_bootstrap_servers: String,
    #[arg(long, env = "TICKETS_URL", default_value = "http://tickets:8094")]
    tickets_url: String,
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8092")]
    listen_addr: SocketAddr,
    /// This bot as Mattermost reaches it, for button callbacks.
    #[arg(long, env = "CALLBACK_URL", default_value = "http://chatops-bot:8092")]
    callback_url: String,
    /// Grafana and the tickets UI as people reach them, for links on cards.
    #[arg(long, env = "GRAFANA_PUBLIC_URL", default_value = "http://localhost:3000")]
    grafana_public_url: String,
    #[arg(long, env = "TICKETS_PUBLIC_URL", default_value = "http://localhost:8094")]
    tickets_public_url: String,
    #[arg(long, env = "CICD_URL", default_value = "http://cicd-sim:8091")]
    cicd_url: String,
    #[arg(long, env = "FLINK_URL", default_value = "http://jobmanager:8081")]
    flink_url: String,
    /// Claude Code's key. Without it, `@ops-assistant` says it isn't set up.
    #[arg(long, env = "ANTHROPIC_API_KEY", hide_env_values = true)]
    anthropic_api_key: Option<String>,
    #[arg(long, env = "CLAUDE_BIN", default_value = "claude")]
    claude_bin: String,
    #[arg(long, env = "CLAUDE_MODEL", default_value = "claude-sonnet-5-5")]
    claude_model: String,
    /// Claude Code's home directory, where its sessions live.
    #[arg(long, env = "CLAUDE_HOME", default_value = "/var/lib/claude")]
    claude_home: PathBuf,
    #[arg(long, env = "CLAUDE_TIMEOUT", default_value = "5m", value_parser = humantime::parse_duration)]
    claude_timeout: Duration,
    /// The MCP server's view of the platform: the ops DB as a read-only
    /// role, Prometheus and Kafka.
    #[arg(long, env = "ASSISTANT_DATABASE_URL", default_value = "postgres://assistant:assistant@postgres:5432/ops")]
    assistant_database_url: String,
    #[arg(long, env = "PROMETHEUS_URL", default_value = "http://prometheus:9090")]
    prometheus_url: String,
}

type AppState = State<Arc<Chatops>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = Config::parse();

    let mm = mattermost::Mattermost::new(&config.mattermost_url);
    let admin = setup::Admin { username: config.admin_user.clone(), password: config.admin_password.clone() };
    let workspace = setup::bootstrap(&mm, &admin, &config.team).await.context("setting up Mattermost")?;
    let links = cards::Links {
        grafana: config.grafana_public_url.trim_end_matches('/').to_string(),
        tickets: config.tickets_public_url.trim_end_matches('/').to_string(),
        callback: config.callback_url.trim_end_matches('/').to_string(),
    };
    let assistant = match config.anthropic_api_key.as_deref().filter(|k| !k.is_empty()) {
        Some(_) => Some(assistant::Assistant::new(assistant::AssistantConfig {
            claude: config.claude_bin.clone(),
            model: config.claude_model.clone(),
            home: config.claude_home.clone(),
            mcp_env: HashMap::from([
                ("OPS_DATABASE_URL".to_string(), config.assistant_database_url.clone()),
                ("PROMETHEUS_URL".to_string(), config.prometheus_url.clone()),
                ("FLINK_URL".to_string(), config.flink_url.clone()),
                ("CICD_URL".to_string(), config.cicd_url.clone()),
                ("TICKETS_URL".to_string(), config.tickets_url.clone()),
                ("CHATOPS_URL".to_string(), format!("http://localhost:{}", config.listen_addr.port())),
                ("KAFKA_BOOTSTRAP_SERVERS".to_string(), config.kafka_bootstrap_servers.clone()),
            ]),
            timeout: config.claude_timeout,
        })),
        None => {
            warn!("ANTHROPIC_API_KEY isn't set; @ops-assistant is off");
            None
        }
    };
    let chatops = Arc::new(Chatops::new(
        mm,
        workspace,
        admin,
        links,
        Publisher::new(&config.kafka_bootstrap_servers, "chatops-bot")?,
        &config.tickets_url,
        CicdClient::new(&config.cicd_url),
        FlinkClient::new(&config.flink_url),
        assistant,
    ));

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/posts", post(create_post))
        .route("/api/posts/{post_id}", put(update_post))
        .route("/api/proposals", post(propose))
        .route("/actions", post(action))
        .route("/hooks/tickets", post(ticket_hook))
        .route("/admin/reset-channels", post(reset_channels))
        .route("/admin/transcript", get(transcript))
        .route("/admin/replay/posts", post(replay_post))
        .route("/admin/replay/posts/{post_id}", put(replay_edit))
        .with_state(chatops.clone());
    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %config.listen_addr, "chatops-bot listening");

    let mut tasks = JoinSet::new();
    tasks.spawn(async move { axum::serve(listener, app).await.context("HTTP server") });
    tasks.spawn(firehose(config.kafka_bootstrap_servers.clone(), chatops.clone()));
    tasks.spawn(events(chatops));
    tokio::select! {
        Some(result) = tasks.join_next() => {
            result.context("chatops-bot task panicked")??;
            anyhow::bail!("a chatops-bot task stopped unexpectedly");
        }
        _ = shutdown_signal() => info!("shutting down"),
    }
    Ok(())
}

fn error(status: StatusCode, err: anyhow::Error) -> Response {
    warn!(error = format!("{err:#}"), "request failed");
    (status, Json(json!({"error": format!("{err:#}")}))).into_response()
}

async fn create_post(State(chatops): AppState, Json(request): Json<PostRequest>) -> Response {
    match chatops.post(&request).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => error(StatusCode::BAD_GATEWAY, err),
    }
}

async fn update_post(
    State(chatops): AppState,
    Path(post_id): Path<String>,
    Json(request): Json<PostRequest>,
) -> Response {
    match chatops.update(&post_id, &request).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => error(StatusCode::BAD_GATEWAY, err),
    }
}

async fn propose(State(chatops): AppState, Json(request): Json<ProposalRequest>) -> Response {
    match chatops.propose(request).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => error(StatusCode::BAD_GATEWAY, err),
    }
}

async fn action(State(chatops): AppState, Json(request): Json<serde_json::Value>) -> Response {
    // A replayed card's buttons change nothing; the recording shows what
    // the click did.
    if bot::is_replayed_click(&request) {
        return Json(json!({})).into_response();
    }
    let request: ActionRequest = match serde_json::from_value(request) {
        Ok(request) => request,
        Err(err) => return error(StatusCode::UNPROCESSABLE_ENTITY, err.into()),
    };
    let (action_id, phase) = (request.context.action_id, request.context.button.phase);
    match chatops.on_action(request).await {
        Ok((body, user)) => {
            // A proposal's approval is carried out in the background; the
            // click gets its answer at once.
            if !user.is_empty() {
                let chatops = chatops.clone();
                tokio::spawn(async move { chatops.carry_out(action_id, phase, &user).await });
            }
            Json(body).into_response()
        }
        Err(err) => {
            let body = json!({"ephemeral_text": format!("Couldn't record that: {err:#}")});
            warn!(error = format!("{err:#}"), "button click failed");
            Json(body).into_response()
        }
    }
}

async fn ticket_hook(State(chatops): AppState, Json(notification): Json<TicketNotification>) -> Response {
    match chatops.on_ticket(notification).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => error(StatusCode::BAD_GATEWAY, err),
    }
}

async fn reset_channels(State(chatops): AppState) -> Response {
    match chatops.reset().await {
        Ok(()) => Json(json!({"reset": true})).into_response(),
        Err(err) => error(StatusCode::BAD_GATEWAY, err),
    }
}

async fn transcript(State(chatops): AppState) -> Response {
    Json(chatops.transcript().await).into_response()
}

async fn replay_post(State(chatops): AppState, Json(request): Json<ReplayPost>) -> Response {
    match chatops.replay_post(&request).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => error(StatusCode::BAD_GATEWAY, err),
    }
}

async fn replay_edit(
    State(chatops): AppState,
    Path(post_id): Path<String>,
    Json(request): Json<ReplayEdit>,
) -> Response {
    match chatops.replay_edit(&post_id, &request).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => error(StatusCode::BAD_GATEWAY, err),
    }
}

/// Posts every alert to `#platform-alerts`. It starts from the latest
/// offsets on first run, so a new channel doesn't replay history.
async fn firehose(bootstrap_servers: String, chatops: Arc<Chatops>) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap_servers)
        .set("group.id", "chatops-bot")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .create()
        .context("creating the Kafka consumer")?;
    consumer.subscribe(&[topics::ALERTS])?;
    info!(topic = topics::ALERTS, "posting the alert firehose");
    loop {
        let message = match consumer.recv().await {
            Ok(message) => message,
            Err(err) => {
                warn!(%err, "Kafka consumer error");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let Some(payload) = message.payload() else { continue };
        let alert: AlertEvent = match serde_json::from_slice(payload) {
            Ok(alert) => alert,
            Err(err) => {
                warn!(%err, offset = message.offset(), "skipping an undecodable alert");
                continue;
            }
        };
        if let Err(err) = chatops.post_alert(alert).await {
            warn!(error = format!("{err:#}"), "posting an alert");
        }
    }
}

/// Follows Mattermost's WebSocket as the admin, who is in every platform
/// channel: records every post and edit in the transcript, and hands new
/// posts to the bot. Reconnects (logging in again)
/// when the connection drops.
async fn events(chatops: Arc<Chatops>) -> anyhow::Result<()> {
    loop {
        if let Err(err) = listen(&chatops).await {
            warn!(error = format!("{err:#}"), "Mattermost WebSocket dropped; reconnecting");
            if let Err(err) = chatops.relogin().await {
                warn!(error = format!("{err:#}"), "logging in again");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn listen(chatops: &Arc<Chatops>) -> anyhow::Result<()> {
    let token = chatops.workspace.read().await.admin_token.clone();
    let mut request = chatops.mm.websocket_url().into_client_request()?;
    request.headers_mut().insert("Authorization", format!("Bearer {token}").parse()?);
    let (mut stream, _) = tokio_tungstenite::connect_async(request).await.context("connecting")?;
    info!("following Mattermost events");
    while let Some(message) = stream.next().await {
        let tungstenite::Message::Text(text) = message? else { continue };
        let Ok(event) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
        let edit = match event["event"].as_str() {
            Some("posted") => false,
            Some("post_edited") => true,
            _ => continue,
        };
        let Some(post) = event["data"]["post"].as_str().and_then(|p| serde_json::from_str::<mattermost::Post>(p).ok())
        else {
            continue;
        };
        chatops.record(&post, edit).await;
        if !edit {
            let chatops = chatops.clone();
            tokio::spawn(async move { chatops.on_posted(post).await });
        }
    }
    anyhow::bail!("the WebSocket closed")
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
