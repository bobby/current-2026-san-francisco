//! Plays a recording back (docs/design.md §10.3): the fallback for when the
//! live stack, Jev, Claude or the network misbehaves on stage.
//!
//! `director play` runs in the Compose network as the `replay` service, which
//! `director replay start` starts once it has quieted the platform. It
//! follows a clock over the recording, which `pause`, `resume` and `seek`
//! move (`:8096`), and at each moment:
//!
//! - posts and edits the transcript's posts through the chatops-bot, as
//!   their authors, mapping each recorded post to its new one so replies
//!   land in the right threads;
//! - inserts the recorded rows into the ops and tickets databases, with
//!   their timestamps moved to now, so Grafana's counters and the tickets UI
//!   follow along;
//! - serves the real jobs' recorded Flink metrics on `:9249`, as they were at
//!   that moment, for Prometheus to scrape (its `replay` job).
//!
//! Starting partway (`--from`) or seeking forward applies everything before
//! the new moment at once, except the alert firehose's posts older than a
//! couple of minutes, which only scroll past.

use std::{collections::HashMap, fmt::Write as _, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{Query, State},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use platform_clients::chatops::{Channel, ChatopsClient, ReplayEdit, ReplayPost};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::recording::{Database, Recording};

/// While catching up, firehose posts older than this are skipped.
const FIREHOSE_CATCH_UP: f64 = 120.0;

/// The replay's clock, in seconds of the recording.
#[derive(Debug, Clone)]
pub struct Clock {
    /// The offset at `since`.
    base: f64,
    since: DateTime<Utc>,
    speed: f64,
    paused: bool,
    end: f64,
}

impl Clock {
    pub fn new(from: f64, speed: f64, end: f64, now: DateTime<Utc>) -> Self {
        Self { base: from.clamp(0.0, end), since: now, speed, paused: false, end }
    }

    pub fn at(&self, now: DateTime<Utc>) -> f64 {
        if self.paused {
            return self.base;
        }
        let elapsed = (now - self.since).num_milliseconds() as f64 / 1000.0;
        (self.base + elapsed * self.speed).min(self.end)
    }

    pub fn pause(&mut self, now: DateTime<Utc>) {
        self.base = self.at(now);
        self.since = now;
        self.paused = true;
    }

    pub fn resume(&mut self, now: DateTime<Utc>) {
        self.base = self.at(now);
        self.since = now;
        self.paused = false;
    }

    /// Moves forward only: what's been shown can't be taken back.
    pub fn seek(&mut self, to: f64, now: DateTime<Utc>) {
        let current = self.at(now);
        self.base = to.clamp(current, self.end);
        self.since = now;
    }

    /// When the recording's offset `at` happens (or happened) by this clock:
    /// a row recorded at `at` gets this time.
    pub fn when(&self, at: f64, now: DateTime<Utc>) -> DateTime<Utc> {
        let behind = (self.at(now) - at) / self.speed;
        now - chrono::Duration::milliseconds((behind * 1000.0) as i64)
    }
}

/// What `GET /status` reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Status {
    pub recording: String,
    pub offset: f64,
    pub end: f64,
    pub speed: f64,
    pub paused: bool,
    pub finished: bool,
    pub posts: usize,
    pub edits: usize,
    pub rows: usize,
    /// The next timeline step, if any: (offset, what).
    pub next_step: Option<(f64, String)>,
}

/// One thing to do at a moment of the recording.
#[derive(Debug, Clone)]
enum Event {
    /// The transcript entry at this index.
    Chat(usize),
    /// Row `row` of table `table`.
    Row { table: usize, row: usize },
}

struct Player {
    recording: Recording,
    clock: Mutex<Clock>,
    /// Everything to do, in time order.
    events: Vec<(f64, Event)>,
    progress: Mutex<Progress>,
}

#[derive(Default)]
struct Progress {
    /// Events done so far.
    next: usize,
    /// Recorded post ID → the replayed post's ID.
    posts: HashMap<String, String>,
    posted: usize,
    edited: usize,
    rows: usize,
}

pub struct PlayOptions {
    pub from: Duration,
    pub speed: f64,
    pub chatops_url: String,
    pub ops_database_url: String,
    pub tickets_database_url: String,
    pub metrics_addr: SocketAddr,
    pub control_addr: SocketAddr,
}

/// `director play`: plays the recording until stopped.
pub async fn play(path: &std::path::Path, options: PlayOptions) -> anyhow::Result<()> {
    let recording = Recording::load(path)?;
    anyhow::ensure!(options.speed > 0.0, "the speed must be positive");
    info!(recording = recording.describe(), "replaying");
    let events = schedule(&recording);
    let clock = Clock::new(options.from.as_secs_f64(), options.speed, recording.length(), Utc::now());
    let player = Arc::new(Player { recording, clock: Mutex::new(clock), events, progress: Default::default() });

    let chatops = ChatopsClient::new(&options.chatops_url);
    let ops = PgPool::connect(&options.ops_database_url).await.context("connecting to the ops DB")?;
    let tickets = PgPool::connect(&options.tickets_database_url).await.context("connecting to the tickets DB")?;

    let metrics = Router::new().route("/metrics", get(metrics)).with_state(player.clone());
    let control = Router::new()
        .route("/status", get(status))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/seek", post(seek))
        .with_state(player.clone());
    let metrics_listener = tokio::net::TcpListener::bind(options.metrics_addr).await?;
    let control_listener = tokio::net::TcpListener::bind(options.control_addr).await?;
    tokio::spawn(async move { axum::serve(metrics_listener, metrics).await });
    tokio::spawn(async move { axum::serve(control_listener, control).await });

    loop {
        // The progress lock is held one event at a time, so the control API
        // answers while a seek catches up.
        loop {
            let at = player.clock.lock().await.at(Utc::now());
            let mut progress = player.progress.lock().await;
            let Some((offset, event)) = player.events.get(progress.next).cloned() else { break };
            if offset > at {
                break;
            }
            let result = match event {
                Event::Chat(i) => player.chat(&chatops, i, at, &mut progress).await,
                Event::Row { table, row } => {
                    let when = player.clock.lock().await.when(offset, Utc::now());
                    let pool = match player.recording.tables[table].database {
                        Database::Ops => &ops,
                        Database::Tickets => &tickets,
                    };
                    player.insert(pool, table, row, when).await.map(|()| progress.rows += 1)
                }
            };
            if let Err(err) = result {
                warn!(error = format!("{err:#}"), offset, "replaying an event");
            }
            progress.next += 1;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Every chat entry and row, in time order; for equal times, rows (a ticket
/// before its journal) before the posts that show them.
fn schedule(recording: &Recording) -> Vec<(f64, Event)> {
    let mut events: Vec<(f64, Event)> = Vec::new();
    for (table, t) in recording.tables.iter().enumerate() {
        for (row, value) in t.rows.iter().enumerate() {
            let Some(ts) = value[&t.time_column].as_str().and_then(parse_ts) else { continue };
            events.push((recording.offset(ts), Event::Row { table, row }));
        }
    }
    for (i, entry) in recording.transcript.iter().enumerate() {
        events.push((recording.offset(entry.ts), Event::Chat(i)));
    }
    // A stable sort keeps tables in order, and rows before posts.
    events.sort_by(|a, b| a.0.total_cmp(&b.0));
    events
}

/// Postgres's `row_to_json` timestamps, e.g. `2026-10-22T15:05:58.123+00:00`.
fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%#z"))
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

impl Player {
    async fn chat(&self, chatops: &ChatopsClient, i: usize, at: f64, progress: &mut Progress) -> anyhow::Result<()> {
        let entry = &self.recording.transcript[i];
        if entry.edit {
            // An edit of a post that wasn't replayed is skipped with it.
            let Some(post_id) = progress.posts.get(&entry.post_id) else { return Ok(()) };
            chatops
                .replay_edit(post_id, &ReplayEdit { message: entry.message.clone(), props: entry.props.clone() })
                .await?;
            progress.edited += 1;
            return Ok(());
        }
        let offset = self.recording.offset(entry.ts);
        if entry.channel == Channel::PlatformAlerts && at - offset > FIREHOSE_CATCH_UP {
            return Ok(());
        }
        let root_id = match &entry.root_id {
            Some(root) => match progress.posts.get(root) {
                Some(new) => Some(new.clone()),
                None => return Ok(()),
            },
            None => None,
        };
        let request = ReplayPost {
            channel: entry.channel,
            user: entry.user.clone(),
            root_id,
            message: entry.message.clone(),
            props: entry.props.clone(),
        };
        let posted = chatops.replay_post(&request).await?;
        progress.posts.insert(entry.post_id.clone(), posted.post_id);
        progress.posted += 1;
        Ok(())
    }

    /// Inserts a recorded row with its timestamps moved by the same amount,
    /// so its time column reads `when`.
    async fn insert(&self, pool: &PgPool, table: usize, row: usize, when: DateTime<Utc>) -> anyhow::Result<()> {
        let t = &self.recording.tables[table];
        let mut value = t.rows[row].clone();
        let recorded = value[&t.time_column].as_str().and_then(parse_ts).context("the row has no time")?;
        let shift = when - recorded;
        for column in &t.timestamp_columns {
            if let Some(ts) = value[column].as_str().and_then(parse_ts) {
                value[column] = json!((ts + shift).to_rfc3339());
            }
        }
        anyhow::ensure!(
            crate::recording::TABLES.iter().any(|(_, name, _)| *name == t.table),
            "{} isn't a table replays fill",
            t.table
        );
        let sql = format!(
            "INSERT INTO {0} OVERRIDING SYSTEM VALUE SELECT * FROM jsonb_populate_record(NULL::{0}, $1) ON CONFLICT DO NOTHING",
            t.table
        );
        sqlx::query(sqlx::AssertSqlSafe(sql)).bind(value).execute(pool).await?;
        Ok(())
    }

    async fn status(&self) -> Status {
        let clock = self.clock.lock().await.clone();
        let offset = clock.at(Utc::now());
        let progress = self.progress.lock().await;
        Status {
            recording: self.recording.describe(),
            offset,
            end: clock.end,
            speed: clock.speed,
            paused: clock.paused,
            finished: progress.next >= self.events.len(),
            posts: progress.posted,
            edits: progress.edited,
            rows: progress.rows,
            next_step: self
                .recording
                .steps
                .iter()
                .find(|s| s.at_secs as f64 > offset)
                .map(|s| (s.at_secs as f64, format!("{} — {}", s.action, s.note))),
        }
    }
}

/// The recorded Flink metrics as they were at the clock's moment, in
/// Prometheus's text format. A series that had gone quiet (its job
/// restarted or rescaled) isn't served.
async fn metrics(State(player): State<Arc<Player>>) -> String {
    let at = player.clock.lock().await.at(Utc::now());
    let fresh = 2.0 * crate::recording::STEP.as_secs_f64();
    let mut out = String::new();
    for series in &player.recording.series {
        let latest = series.points.partition_point(|(t, _)| *t <= at);
        let Some((t, value)) = latest.checked_sub(1).map(|i| series.points[i]) else { continue };
        if at - t > fresh {
            continue;
        }
        let Some(name) = series.labels.get("__name__") else { continue };
        let labels: Vec<String> = series
            .labels
            .iter()
            .filter(|(k, _)| *k != "__name__")
            .map(|(k, v)| format!("{k}=\"{}\"", v.replace('\\', r"\\").replace('"', "\\\"").replace('\n', r"\n")))
            .collect();
        let _ = writeln!(out, "{name}{{{}}} {value}", labels.join(","));
    }
    out
}

async fn status(State(player): State<Arc<Player>>) -> Json<Status> {
    Json(player.status().await)
}

async fn pause(State(player): State<Arc<Player>>) -> Json<Status> {
    player.clock.lock().await.pause(Utc::now());
    Json(player.status().await)
}

async fn resume(State(player): State<Arc<Player>>) -> Json<Status> {
    player.clock.lock().await.resume(Utc::now());
    Json(player.status().await)
}

#[derive(Debug, Deserialize)]
struct SeekTo {
    /// An offset into the recording, e.g. `15m` or `15m30s`.
    to: String,
}

async fn seek(State(player): State<Arc<Player>>, Query(q): Query<SeekTo>) -> Json<Value> {
    match humantime::parse_duration(&q.to) {
        Ok(to) => {
            player.clock.lock().await.seek(to.as_secs_f64(), Utc::now());
            Json(json!(player.status().await))
        }
        Err(err) => Json(json!({"error": format!("{}: {err}", q.to)})),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn the_clock_runs_pauses_and_seeks_forward() {
        let mut clock = Clock::new(60.0, 2.0, 600.0, t(0));
        assert_eq!(clock.at(t(10)), 80.0);
        clock.pause(t(10));
        assert_eq!(clock.at(t(100)), 80.0);
        clock.resume(t(100));
        assert_eq!(clock.at(t(110)), 100.0);
        clock.seek(50.0, t(110));
        assert_eq!(clock.at(t(110)), 100.0, "never backwards");
        clock.seek(300.0, t(110));
        assert_eq!(clock.at(t(115)), 310.0);
        assert_eq!(clock.at(t(1000)), 600.0, "stops at the end");
        // At double speed, something recorded 20 s before the clock happened
        // 10 s ago.
        assert_eq!(clock.when(290.0, t(115)), t(105));
    }

    #[test]
    fn parses_postgres_timestamps() {
        assert_eq!(parse_ts("2026-10-22T15:05:58+00:00"), DateTime::from_timestamp(1_792_681_558, 0));
        assert!(parse_ts("2026-10-22T15:05:58.123456+00:00").is_some());
        assert!(parse_ts("not a time").is_none());
    }
}
