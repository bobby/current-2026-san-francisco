//! The incident table in Postgres (the `tickets` database).

use platform_clients::tickets::{Incident, JournalEntry};
use serde_json::{Map, Value};
use sqlx::{AssertSqlSafe, PgPool, Postgres, QueryBuilder, postgres::PgPoolOptions, types::Json};

use crate::query::{self, COLUMNS, Kind, Query};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("tickets DB query failed: {0}")]
    Db(#[from] sqlx::Error),
    #[error("invalid {field}: {message}")]
    Invalid { field: String, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// ServiceNow's display format for times.
const TIME_FORMAT: &str = "'YYYY-MM-DD HH24:MI:SS'";

/// Every incident column as ServiceNow serves it (strings, times formatted,
/// empty when unset), plus the journals, as one JSON object per row.
fn select_incidents() -> String {
    let columns: Vec<String> = COLUMNS
        .iter()
        .map(|c| match c.kind {
            Kind::Text => format!("i.{0}", c.name),
            Kind::Int => format!("i.{0}::text AS {0}", c.name),
            Kind::Time => format!("coalesce(to_char(i.{0} AT TIME ZONE 'UTC', {TIME_FORMAT}), '') AS {0}", c.name),
        })
        .collect();
    let journal = |element: &str, label: &str| {
        format!(
            "coalesce((SELECT string_agg(to_char(j.created_on AT TIME ZONE 'UTC', {TIME_FORMAT}) || ' - ' || j.created_by \
             || ' ({label})' || E'\\n' || j.value, E'\\n\\n' ORDER BY j.id DESC) \
             FROM journal j WHERE j.sys_id = i.sys_id AND j.element = '{element}'), '') AS {element}"
        )
    };
    format!(
        "SELECT to_jsonb(x) FROM (SELECT {}, {}, {} FROM incident i",
        columns.join(", "),
        journal("work_notes", "Work notes"),
        journal("comments", "Additional comments"),
    )
}

/// What a create or update changed.
pub struct Change {
    pub incident: Incident,
    pub changed: Vec<String>,
    pub note: Option<JournalEntry>,
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new().max_connections(8).connect(url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> std::result::Result<(), sqlx::migrate::MigrateError> {
        MIGRATOR.run(&self.pool).await
    }

    pub async fn get(&self, sys_id: &str) -> Result<Option<Incident>> {
        self.find_one("sys_id", sys_id).await
    }

    pub async fn get_by_number(&self, number: &str) -> Result<Option<Incident>> {
        self.find_one("number", number).await
    }

    async fn find_one(&self, column: &str, value: &str) -> Result<Option<Incident>> {
        let sql = format!("{} WHERE i.{column} = $1) x", select_incidents());
        let row: Option<(Json<Incident>,)> =
            sqlx::query_as(AssertSqlSafe(sql)).bind(value).fetch_optional(&self.pool).await?;
        Ok(row.map(|(Json(incident),)| incident))
    }

    /// The incidents matching `query`, and how many match in all.
    pub async fn query(&self, query: &Query, limit: i64, offset: i64) -> Result<(Vec<Incident>, i64)> {
        let mut qb = QueryBuilder::<Postgres>::new(select_incidents());
        query.push_where(&mut qb);
        query.push_order(&mut qb);
        qb.push(" LIMIT ").push_bind(limit).push(" OFFSET ").push_bind(offset).push(") x");
        let rows: Vec<(Json<Incident>,)> = qb.build_query_as().fetch_all(&self.pool).await?;

        let mut count = QueryBuilder::<Postgres>::new("SELECT count(*) FROM incident");
        query.push_where(&mut count);
        let (total,): (i64,) = count.build_query_as().fetch_one(&self.pool).await?;
        Ok((rows.into_iter().map(|(Json(i),)| i).collect(), total))
    }

    /// The journal, oldest first.
    pub async fn journal(&self, sys_id: &str) -> Result<Vec<JournalEntry>> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT element, value, created_by, to_char(created_on AT TIME ZONE 'UTC', {TIME_FORMAT}) \
             FROM journal WHERE sys_id = $1 ORDER BY id"
        )))
        .bind(sys_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(element, value, created_by, created_on)| JournalEntry { element, value, created_by, created_on })
            .collect())
    }

    pub async fn create(&self, fields: &Map<String, Value>, user: &str) -> Result<Change> {
        let sys_id = uuid::Uuid::new_v4().simple().to_string();
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO incident (sys_id, sys_created_by, sys_updated_by) VALUES ($1, $2, $2)")
            .bind(&sys_id)
            .bind(user)
            .execute(&mut *tx)
            .await?;
        let (changed, note) = apply(&mut tx, &sys_id, fields, user, false).await?;
        tx.commit().await?;
        let incident = self.get(&sys_id).await?.ok_or(sqlx::Error::RowNotFound)?;
        Ok(Change { incident, changed, note })
    }

    /// Updates an incident; `None` if there is no such incident.
    pub async fn update(&self, sys_id: &str, fields: &Map<String, Value>, user: &str) -> Result<Option<Change>> {
        let mut tx = self.pool.begin().await?;
        let exists: Option<(String,)> = sqlx::query_as("SELECT sys_id FROM incident WHERE sys_id = $1 FOR UPDATE")
            .bind(sys_id)
            .fetch_optional(&mut *tx)
            .await?;
        if exists.is_none() {
            return Ok(None);
        }
        let (changed, note) = apply(&mut tx, sys_id, fields, user, true).await?;
        tx.commit().await?;
        let incident = self.get(sys_id).await?.ok_or(sqlx::Error::RowNotFound)?;
        Ok(Some(Change { incident, changed, note }))
    }
}

/// A field's value as a string, accepting JSON strings, numbers and booleans
/// as ServiceNow does.
fn as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Sets the writable fields present in `fields` and adds any journal entry.
/// Unknown and read-only fields are ignored, as ServiceNow does.
async fn apply(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    sys_id: &str,
    fields: &Map<String, Value>,
    user: &str,
    bump: bool,
) -> Result<(Vec<String>, Option<JournalEntry>)> {
    let mut changed = Vec::new();
    let mut qb = QueryBuilder::<Postgres>::new("UPDATE incident SET sys_updated_on = now(), sys_updated_by = ");
    qb.push_bind(user.to_string());
    if bump {
        qb.push(", sys_mod_count = sys_mod_count + 1");
    }
    for column in COLUMNS.iter().filter(|c| c.writable) {
        let Some(value) = fields.get(column.name).and_then(as_string) else { continue };
        qb.push(format!(", {} = ", column.name));
        match column.kind {
            Kind::Int => {
                let n =
                    query::int(&value).map_err(|message| Error::Invalid { field: column.name.to_string(), message })?;
                qb.push_bind(n as i16);
            }
            _ => {
                qb.push_bind(value);
            }
        }
        changed.push(column.name.to_string());
    }
    qb.push(" WHERE sys_id = ").push_bind(sys_id.to_string());
    qb.build().execute(&mut **tx).await?;
    // Resolving or closing stamps the time, once; reopening clears it. A
    // separate statement, since SET expressions see the old state.
    sqlx::query(
        "UPDATE incident SET resolved_at = CASE WHEN state IN (6, 7) THEN coalesce(resolved_at, now()) END, \
         closed_at = CASE WHEN state = 7 THEN coalesce(closed_at, now()) END WHERE sys_id = $1",
    )
    .bind(sys_id)
    .execute(&mut **tx)
    .await?;

    let mut note = None;
    for element in ["work_notes", "comments"] {
        let Some(value) = fields.get(element).and_then(as_string).filter(|v| !v.trim().is_empty()) else { continue };
        let (created_on,): (String,) = sqlx::query_as(AssertSqlSafe(format!(
            "INSERT INTO journal (sys_id, element, value, created_by) VALUES ($1, $2, $3, $4) \
             RETURNING to_char(created_on AT TIME ZONE 'UTC', {TIME_FORMAT})"
        )))
        .bind(sys_id)
        .bind(element)
        .bind(&value)
        .bind(user)
        .fetch_one(&mut **tx)
        .await?;
        changed.push(element.to_string());
        note = Some(JournalEntry { element: element.to_string(), value, created_by: user.to_string(), created_on });
    }
    Ok((changed, note))
}
