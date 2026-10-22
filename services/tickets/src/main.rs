//! The tickets mock (docs/design.md §9.2): an incident table with a
//! ServiceNow Table API-shaped interface, so real ServiceNow clients work
//! against it.
//!
//! - `POST /api/now/table/incident`: create an incident.
//! - `GET /api/now/table/incident?sysparm_query=…&sysparm_limit=…&sysparm_offset=…&sysparm_fields=…`
//! - `GET`, `PATCH` or `PUT /api/now/table/incident/{sys_id}`; setting
//!   `work_notes` or `comments` adds a journal entry.
//! - `GET /` and `GET /incident/{number}`: the web UI.
//!
//! Requests authenticate with HTTP basic auth, as with ServiceNow. Passwords
//! aren't checked: the user name is recorded as the author of the change.
//! After every change it tells the chatops-bot (`NOTIFY_URL`), which keeps
//! the ticket's card in Mattermost current.

mod query;
mod store;
mod ui;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use base64::Engine;
use clap::Parser;
use platform_clients::tickets::{TicketEvent, TicketNotification};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use store::{Change, Store};
use tracing::{info, warn};

#[derive(Debug, Clone, Parser)]
#[command(about = "A ServiceNow Table API-shaped incident tracker")]
struct Config {
    #[arg(long, env = "TICKETS_DATABASE_URL", default_value = "postgres://tickets:tickets@postgres:5432/tickets")]
    database_url: String,
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8094")]
    listen_addr: SocketAddr,
    /// Where people reach the UI, for links and Open Graph tags.
    #[arg(long, env = "TICKETS_PUBLIC_URL", default_value = "http://localhost:8094")]
    public_url: String,
    /// Where to send each change (the chatops-bot's ticket hook).
    #[arg(long, env = "NOTIFY_URL")]
    notify_url: Option<String>,
}

struct App {
    store: Store,
    public_url: String,
    notify_url: Option<String>,
    http: reqwest::Client,
}

type AppState = State<Arc<App>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();
    let config = Config::parse();
    let store = connect(&config.database_url).await?;
    let app = Arc::new(App {
        store,
        public_url: config.public_url.trim_end_matches('/').to_string(),
        notify_url: config.notify_url.clone(),
        http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
    });

    let router = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/now/table/incident", get(list).post(create))
        .route("/api/now/table/incident/{sys_id}", get(read).patch(update).put(update))
        .route("/", get(ui_list))
        .route("/incident/{number}", get(ui_detail))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %config.listen_addr, notify_url = ?config.notify_url, "tickets listening");
    axum::serve(listener, router).with_graceful_shutdown(shutdown_signal()).await?;
    Ok(())
}

/// Connects and migrates, retrying while Postgres starts up.
async fn connect(url: &str) -> anyhow::Result<Store> {
    let mut attempt = 0;
    loop {
        match Store::connect(url).await {
            Ok(store) => {
                store.migrate().await.context("migrating the tickets DB")?;
                return Ok(store);
            }
            Err(err) if attempt < 30 => {
                attempt += 1;
                warn!(%err, attempt, "tickets DB not ready; retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(err) => return Err(err).context("connecting to the tickets DB"),
        }
    }
}

/// The basic-auth user name, or `api` without one.
fn user(headers: &HeaderMap) -> String {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .and_then(|v| base64::engine::general_purpose::STANDARD.decode(v.trim()).ok())
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|v| v.split(':').next().map(str::to_string))
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| "api".to_string())
}

/// ServiceNow's error shape.
fn failure(status: StatusCode, message: &str, detail: &str) -> Response {
    (status, Json(json!({"error": {"message": message, "detail": detail}, "status": "failure"}))).into_response()
}

fn store_failure(err: store::Error) -> Response {
    match err {
        store::Error::Invalid { .. } => failure(StatusCode::BAD_REQUEST, "Invalid field value", &err.to_string()),
        store::Error::Db(_) => {
            warn!(%err, "tickets DB error");
            failure(StatusCode::INTERNAL_SERVER_ERROR, "Internal error", &err.to_string())
        }
    }
}

fn not_found() -> Response {
    failure(StatusCode::NOT_FOUND, "No Record found", "Record doesn't exist or ACL restricts the record retrieval")
}

#[derive(Deserialize)]
struct ListParams {
    #[serde(default)]
    sysparm_query: String,
    sysparm_limit: Option<i64>,
    sysparm_offset: Option<i64>,
    sysparm_fields: Option<String>,
}

#[derive(Deserialize)]
struct FieldParams {
    sysparm_fields: Option<String>,
}

/// The record, restricted to `sysparm_fields` if given.
fn select_fields(incident: &impl serde::Serialize, fields: Option<&str>) -> Value {
    let value = serde_json::to_value(incident).unwrap_or_default();
    let Some(fields) = fields.filter(|f| !f.is_empty()) else { return value };
    let wanted: Vec<&str> = fields.split(',').map(str::trim).collect();
    match value {
        Value::Object(map) => Value::Object(map.into_iter().filter(|(k, _)| wanted.contains(&k.as_str())).collect()),
        other => other,
    }
}

async fn list(State(app): AppState, Query(params): Query<ListParams>) -> Response {
    let query = match query::parse(&params.sysparm_query) {
        Ok(query) => query,
        Err(err) => return failure(StatusCode::BAD_REQUEST, "Invalid sysparm_query", &err),
    };
    let limit = params.sysparm_limit.unwrap_or(1000).clamp(1, 10_000);
    let offset = params.sysparm_offset.unwrap_or(0).max(0);
    match app.store.query(&query, limit, offset).await {
        Ok((incidents, total)) => {
            let result: Vec<Value> =
                incidents.iter().map(|i| select_fields(i, params.sysparm_fields.as_deref())).collect();
            ([("X-Total-Count", total.to_string())], Json(json!({"result": result}))).into_response()
        }
        Err(err) => store_failure(err),
    }
}

async fn read(State(app): AppState, Path(sys_id): Path<String>, Query(params): Query<FieldParams>) -> Response {
    match app.store.get(&sys_id).await {
        Ok(Some(incident)) => {
            Json(json!({"result": select_fields(&incident, params.sysparm_fields.as_deref())})).into_response()
        }
        Ok(None) => not_found(),
        Err(err) => store_failure(err),
    }
}

async fn create(State(app): AppState, headers: HeaderMap, Json(fields): Json<Map<String, Value>>) -> Response {
    match app.store.create(&fields, &user(&headers)).await {
        Ok(change) => {
            info!(number = change.incident.number, by = change.incident.sys_created_by, "incident created");
            let body = Json(json!({"result": change.incident}));
            app.notify(TicketEvent::Created, change);
            (StatusCode::CREATED, body).into_response()
        }
        Err(err) => store_failure(err),
    }
}

async fn update(
    State(app): AppState,
    Path(sys_id): Path<String>,
    headers: HeaderMap,
    Json(fields): Json<Map<String, Value>>,
) -> Response {
    match app.store.update(&sys_id, &fields, &user(&headers)).await {
        Ok(Some(change)) => {
            info!(number = change.incident.number, changed = ?change.changed, "incident updated");
            let body = Json(json!({"result": change.incident}));
            app.notify(TicketEvent::Updated, change);
            body.into_response()
        }
        Ok(None) => not_found(),
        Err(err) => store_failure(err),
    }
}

impl App {
    /// Tells the chatops-bot about a change, in the background: a slow or
    /// missing bot never fails the API call.
    fn notify(&self, event: TicketEvent, change: Change) {
        let Some(url) = self.notify_url.clone() else { return };
        let notification =
            TicketNotification { event, incident: change.incident, changed: change.changed, note: change.note };
        let http = self.http.clone();
        tokio::spawn(async move {
            let result = http.post(&url).json(&notification).send().await.and_then(|r| r.error_for_status());
            if let Err(err) = result {
                warn!(%err, number = notification.incident.number, "notifying the chatops-bot");
            }
        });
    }
}

async fn ui_list(State(app): AppState) -> Response {
    let query =
        query::Query { order: vec![(query::column("number").expect("number column"), true)], ..Default::default() };
    match app.store.query(&query, 100, 0).await {
        Ok((incidents, _)) => Html(ui::list(&incidents, &app.public_url)).into_response(),
        Err(err) => store_failure(err),
    }
}

async fn ui_detail(State(app): AppState, Path(number): Path<String>) -> Response {
    let incident = match app.store.get_by_number(&number).await {
        Ok(Some(incident)) => incident,
        Ok(None) => return (StatusCode::NOT_FOUND, Html(ui::not_found(&number, &app.public_url))).into_response(),
        Err(err) => return store_failure(err),
    };
    match app.store.journal(&incident.sys_id).await {
        Ok(journal) => Html(ui::detail(&incident, &journal, &app.public_url)).into_response(),
        Err(err) => store_failure(err),
    }
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
