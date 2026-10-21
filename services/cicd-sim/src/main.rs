//! cicd-sim (docs/design.md §4.3): the platform's deployment pipeline.
//!
//! - `GET /catalogue`: the version catalogue (`jobs/versions.yaml`).
//! - `POST /deploy` `{job, version, args?, triggered_by?, action_id?, ...}`
//! - `POST /rollback` `{job, to_version?, triggered_by?, action_id?, ...}`
//! - `POST /baseline` `{jobs?, triggered_by?}`: deploy each job's baseline
//!   version unless it is already running.
//!
//! Deploys and rollbacks answer once Flink has started the new run: 200 with
//! the `platform.deployments` event, or 502 with the event if it failed.

mod catalogue;
mod deployer;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use deployer::{DeployError, DeployRequest, Deployer, RequestContext, RollbackRequest};
use platform_clients::{flink::FlinkClient, kafka::Publisher, ops_db::OpsDb, sql_gateway::SqlGateway};
use platform_model::{Deployment, DeploymentOutcome};
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Clone, Parser)]
#[command(about = "The platform's deployment pipeline")]
struct Config {
    #[arg(long, env = "FLINK_URL", default_value = "http://jobmanager:8081")]
    flink_url: String,
    #[arg(long, env = "SQL_GATEWAY_URL", default_value = "http://sql-gateway:8083")]
    sql_gateway_url: String,
    #[arg(long, env = "KAFKA_BOOTSTRAP_SERVERS", default_value = "kafka:9092")]
    kafka_bootstrap_servers: String,
    #[arg(long, env = "OPS_DATABASE_URL", default_value = "postgres://ops:ops@postgres:5432/ops")]
    ops_database_url: String,
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8091")]
    listen_addr: SocketAddr,
    #[arg(long, env = "CATALOGUE", default_value = "/jobs/versions.yaml")]
    catalogue: PathBuf,
    #[arg(long, env = "JOBS_JAR", default_value = "/jobs/java/target/platform-jobs.jar")]
    jar: PathBuf,
    #[arg(long, env = "SQL_DIR", default_value = "/jobs/sql")]
    sql_dir: PathBuf,
    #[arg(long, env = "SAVEPOINT_TIMEOUT", default_value = "60s", value_parser = humantime::parse_duration)]
    savepoint_timeout: Duration,
    #[arg(long, env = "START_TIMEOUT", default_value = "60s", value_parser = humantime::parse_duration)]
    start_timeout: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();
    let config = Config::parse();

    // The collector owns the ops DB schema; cicd-sim only reads it.
    let db = OpsDb::connect(&config.ops_database_url).await.context("connecting to the ops DB")?;
    let deployer = Deployer::new(
        FlinkClient::new(&config.flink_url),
        SqlGateway::new(&config.sql_gateway_url),
        db,
        Publisher::new(&config.kafka_bootstrap_servers, "cicd-sim")?,
        config.catalogue.clone(),
        config.jar.clone(),
        config.sql_dir.clone(),
        config.savepoint_timeout,
        config.start_timeout,
    );
    deployer.catalogue().context("loading the version catalogue")?;

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/catalogue", get(catalogue))
        .route("/deploy", post(deploy))
        .route("/rollback", post(rollback))
        .route("/baseline", post(baseline))
        .with_state(Arc::new(deployer));
    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    info!(addr = %config.listen_addr, catalogue = %config.catalogue.display(), "cicd-sim listening");
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;
    Ok(())
}

type AppState = State<Arc<Deployer>>;

fn deployment_response(result: Result<Deployment, DeployError>) -> Response {
    match result {
        Ok(d) if d.outcome == DeploymentOutcome::Succeeded => (StatusCode::OK, Json(d)).into_response(),
        Ok(d) => (StatusCode::BAD_GATEWAY, Json(d)).into_response(),
        Err(err) => {
            let status = match err {
                DeployError::UnknownJob(_) | DeployError::UnknownVersion { .. } => StatusCode::NOT_FOUND,
                DeployError::NoPreviousVersion(_) => StatusCode::CONFLICT,
                DeployError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(serde_json::json!({"error": format!("{err:#}")}))).into_response()
        }
    }
}

async fn catalogue(State(deployer): AppState) -> Response {
    match deployer.catalogue() {
        Ok(catalogue) => Json(catalogue).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}")).into_response(),
    }
}

async fn deploy(State(deployer): AppState, Json(request): Json<DeployRequest>) -> Response {
    deployment_response(deployer.deploy(request).await)
}

async fn rollback(State(deployer): AppState, Json(request): Json<RollbackRequest>) -> Response {
    deployment_response(deployer.rollback(request).await)
}

#[derive(Deserialize)]
struct BaselineRequest {
    jobs: Option<Vec<String>>,
    #[serde(flatten)]
    context: RequestContext,
}

async fn baseline(State(deployer): AppState, Json(request): Json<BaselineRequest>) -> Response {
    let results = deployer.baseline(request.jobs, request.context).await;
    let ok = results.iter().all(|r| matches!(r, Ok(d) if d.outcome == DeploymentOutcome::Succeeded));
    let body: Vec<serde_json::Value> = results
        .into_iter()
        .map(|r| match r {
            Ok(d) => serde_json::to_value(d).unwrap_or_default(),
            Err(e) => serde_json::json!({"error": e}),
        })
        .collect();
    (if ok { StatusCode::OK } else { StatusCode::BAD_GATEWAY }, Json(body)).into_response()
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
