//! The incident table's columns, and the Table API's encoded queries
//! (`sysparm_query`), e.g.
//! `cmdb_ci=payments-enrichment^active=true^ORDERBYDESCsys_created_on`.
//!
//! Supported: conditions joined by `^` (AND) with `=`, `!=`, `>`, `>=`, `<`,
//! `<=`, `LIKE`, `NOT LIKE`, `STARTSWITH`, `ENDSWITH`, `IN`, `NOT IN`,
//! `ISEMPTY` and `ISNOTEMPTY`; the `active` pseudo-field; and `ORDERBY` /
//! `ORDERBYDESC`. Anything else is rejected rather than ignored, so a
//! client never gets silently unfiltered results.

use sqlx::{Postgres, QueryBuilder};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Text,
    Int,
    Time,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    /// Clients can set it on create and update.
    pub writable: bool,
}

const fn col(name: &'static str, kind: Kind, writable: bool) -> Column {
    Column { name, kind, writable }
}

pub const COLUMNS: &[Column] = &[
    col("sys_id", Kind::Text, false),
    col("number", Kind::Text, false),
    col("short_description", Kind::Text, true),
    col("description", Kind::Text, true),
    col("state", Kind::Int, true),
    col("priority", Kind::Int, true),
    col("impact", Kind::Int, true),
    col("urgency", Kind::Int, true),
    col("category", Kind::Text, true),
    col("assignment_group", Kind::Text, true),
    col("assigned_to", Kind::Text, true),
    col("caller_id", Kind::Text, true),
    col("cmdb_ci", Kind::Text, true),
    col("correlation_id", Kind::Text, true),
    col("close_code", Kind::Text, true),
    col("close_notes", Kind::Text, true),
    col("opened_at", Kind::Time, false),
    col("resolved_at", Kind::Time, false),
    col("closed_at", Kind::Time, false),
    col("sys_created_on", Kind::Time, false),
    col("sys_created_by", Kind::Text, false),
    col("sys_updated_on", Kind::Time, false),
    col("sys_updated_by", Kind::Text, false),
    col("sys_mod_count", Kind::Int, false),
];

pub fn column(name: &str) -> Option<&'static Column> {
    COLUMNS.iter().find(|c| c.name == name)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Like,
    NotLike,
    StartsWith,
    EndsWith,
    In,
    NotIn,
    IsEmpty,
    IsNotEmpty,
}

/// Operator tokens, longest first so `!=` wins over `=` and `NOT LIKE` over
/// `LIKE`.
const OPS: &[(&str, Op)] = &[
    ("ISNOTEMPTY", Op::IsNotEmpty),
    ("ISEMPTY", Op::IsEmpty),
    ("STARTSWITH", Op::StartsWith),
    ("ENDSWITH", Op::EndsWith),
    ("NOT LIKE", Op::NotLike),
    ("NOT IN", Op::NotIn),
    ("LIKE", Op::Like),
    ("IN", Op::In),
    ("!=", Op::Ne),
    (">=", Op::Ge),
    ("<=", Op::Le),
    ("=", Op::Eq),
    (">", Op::Gt),
    ("<", Op::Lt),
];

#[derive(Debug, PartialEq)]
pub enum Term {
    Cond {
        column: &'static Column,
        op: Op,
        value: String,
    },
    /// `active=true`: the incident is still being worked (state below 6).
    Active(bool),
}

#[derive(Debug, Default, PartialEq)]
pub struct Query {
    pub terms: Vec<Term>,
    /// `(column, descending)`
    pub order: Vec<(&'static Column, bool)>,
}

pub fn parse(encoded: &str) -> Result<Query, String> {
    let mut query = Query::default();
    for part in encoded.split('^').map(str::trim).filter(|p| !p.is_empty()) {
        if let Some(name) = part.strip_prefix("ORDERBYDESC") {
            query.order.push((known(name)?, true));
            continue;
        }
        if let Some(name) = part.strip_prefix("ORDERBY") {
            query.order.push((known(name)?, false));
            continue;
        }
        if part == "OR" || part.starts_with("OR") || part.starts_with("NQ") {
            return Err(format!("unsupported query term {part:?}: only AND (^) is supported"));
        }
        let split =
            part.find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')).unwrap_or(part.len());
        let (name, rest) = part.split_at(split);
        let (op, value) = OPS
            .iter()
            .find_map(|(token, op)| rest.strip_prefix(token).map(|value| (*op, value.to_string())))
            .ok_or_else(|| format!("no operator in query term {part:?}"))?;
        if name == "active" {
            let active = match (op, value.as_str()) {
                (Op::Eq, "true") | (Op::Ne, "false") => true,
                (Op::Eq, "false") | (Op::Ne, "true") => false,
                _ => return Err(format!("unsupported active condition {part:?}")),
            };
            query.terms.push(Term::Active(active));
            continue;
        }
        let column = known(name)?;
        validate(column, op, &value).map_err(|e| format!("{part:?}: {e}"))?;
        query.terms.push(Term::Cond { column, op, value });
    }
    Ok(query)
}

fn known(name: &str) -> Result<&'static Column, String> {
    column(name).ok_or_else(|| format!("unknown field {name:?}"))
}

fn validate(column: &Column, op: Op, value: &str) -> Result<(), String> {
    match (column.kind, op) {
        (_, Op::IsEmpty | Op::IsNotEmpty) => Ok(()),
        (Kind::Int, Op::Like | Op::NotLike | Op::StartsWith | Op::EndsWith) => Err("text operator on a number".into()),
        (Kind::Int, Op::In | Op::NotIn) => value.split(',').try_for_each(|v| int(v).map(drop)),
        (Kind::Int, _) => int(value).map(drop),
        (Kind::Time, Op::Eq | Op::Ne | Op::Gt | Op::Ge | Op::Lt | Op::Le) => Ok(()),
        (Kind::Time, _) => Err("unsupported operator on a time".into()),
        (Kind::Text, _) => Ok(()),
    }
}

pub fn int(value: &str) -> Result<i64, String> {
    value.trim().parse().map_err(|_| format!("{value:?} is not a number"))
}

impl Query {
    /// Appends ` WHERE …` (if there are conditions) to `qb`, binding every
    /// value. Column names come only from [`COLUMNS`].
    pub fn push_where(&self, qb: &mut QueryBuilder<Postgres>) {
        for (i, term) in self.terms.iter().enumerate() {
            qb.push(if i == 0 { " WHERE " } else { " AND " });
            match term {
                Term::Active(true) => {
                    qb.push("state < 6");
                }
                Term::Active(false) => {
                    qb.push("state >= 6");
                }
                Term::Cond { column, op, value } => push_cond(qb, column, *op, value),
            }
        }
    }

    pub fn push_order(&self, qb: &mut QueryBuilder<Postgres>) {
        qb.push(" ORDER BY ");
        for (column, desc) in &self.order {
            qb.push(column.name).push(if *desc { " DESC, " } else { " ASC, " });
        }
        qb.push("number ASC");
    }
}

fn push_cond(qb: &mut QueryBuilder<Postgres>, column: &Column, op: Op, value: &str) {
    let name = column.name;
    let comparison = match op {
        Op::Eq => "=",
        Op::Ne => "<>",
        Op::Gt => ">",
        Op::Ge => ">=",
        Op::Lt => "<",
        Op::Le => "<=",
        _ => "",
    };
    match (column.kind, op) {
        (Kind::Text, Op::IsEmpty) => qb.push(format!("{name} = ''")),
        (Kind::Text, Op::IsNotEmpty) => qb.push(format!("{name} <> ''")),
        (_, Op::IsEmpty) => qb.push(format!("{name} IS NULL")),
        (_, Op::IsNotEmpty) => qb.push(format!("{name} IS NOT NULL")),
        (Kind::Text, Op::Like) => qb.push(format!("{name} ILIKE '%' || ")).push_bind(value.to_string()).push(" || '%'"),
        (Kind::Text, Op::NotLike) => {
            qb.push(format!("{name} NOT ILIKE '%' || ")).push_bind(value.to_string()).push(" || '%'")
        }
        (Kind::Text, Op::StartsWith) => qb.push(format!("{name} ILIKE ")).push_bind(value.to_string()).push(" || '%'"),
        (Kind::Text, Op::EndsWith) => qb.push(format!("{name} ILIKE '%' || ")).push_bind(value.to_string()),
        (Kind::Text, Op::In | Op::NotIn) => {
            let values: Vec<String> = value.split(',').map(|v| v.trim().to_string()).collect();
            let negate = if op == Op::NotIn { "NOT " } else { "" };
            qb.push(format!("{negate}{name} = ANY(")).push_bind(values).push(")")
        }
        (Kind::Int, Op::In | Op::NotIn) => {
            let values: Vec<i64> = value.split(',').filter_map(|v| int(v).ok()).collect();
            let negate = if op == Op::NotIn { "NOT " } else { "" };
            qb.push(format!("{negate}{name} = ANY(")).push_bind(values).push(")")
        }
        (Kind::Int, _) => qb.push(format!("{name} {comparison} ")).push_bind(int(value).unwrap_or_default()),
        (Kind::Time, _) => {
            qb.push(format!("{name} {comparison} (")).push_bind(value.to_string()).push(" || ' UTC')::timestamptz")
        }
        (Kind::Text, _) => qb.push(format!("{name} {comparison} ")).push_bind(value.to_string()),
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql(encoded: &str) -> String {
        let query = parse(encoded).unwrap();
        let mut qb = QueryBuilder::new("SELECT * FROM incident");
        query.push_where(&mut qb);
        query.push_order(&mut qb);
        qb.sql().as_str().to_string()
    }

    #[test]
    fn parses_conditions_and_ordering() {
        assert_eq!(
            sql("cmdb_ci=payments-enrichment^active=true^ORDERBYDESCsys_created_on"),
            "SELECT * FROM incident WHERE cmdb_ci = $1 AND state < 6 ORDER BY sys_created_on DESC, number ASC"
        );
        assert_eq!(
            sql("short_descriptionLIKEnull^state!=6^priorityIN1,2"),
            "SELECT * FROM incident WHERE short_description ILIKE '%' || $1 || '%' AND state <> $2 AND priority = ANY($3) ORDER BY number ASC"
        );
        assert_eq!(
            sql("sys_created_on>=2026-10-21 10:00:00^assigned_toISEMPTY"),
            "SELECT * FROM incident WHERE sys_created_on >= ($1 || ' UTC')::timestamptz AND assigned_to = '' ORDER BY number ASC"
        );
        assert_eq!(sql(""), "SELECT * FROM incident ORDER BY number ASC");
    }

    #[test]
    fn rejects_what_it_cannot_honour() {
        for bad in ["nope=1", "state=high", "stateLIKE1", "number=1^ORcmdb_ci=x", "cmdb_ci", "ORDERBYbogus"] {
            assert!(parse(bad).is_err(), "{bad} should be rejected");
        }
    }
}
