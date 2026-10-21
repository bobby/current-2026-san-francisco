#!/usr/bin/env bash
# Creates one database and owning role per service sharing this Postgres
# instance (docs/design.md §5.4). Idempotent: the postgres-init service runs
# it on every `up`, so databases added later also reach existing volumes.
set -euo pipefail

create_db() {
  local name="$1" password="$2"
  psql -v ON_ERROR_STOP=1 --dbname postgres -v name="$name" -v password="$password" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'name', :'password')
  WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'name') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'name', :'name')
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'name') \gexec
SQL
  echo "database $name ready"
}

# A login role that can read every table in a database, including tables
# its owner creates later (the collector's migrations).
create_reader() {
  local name="$1" password="$2" db="$3" owner="$4"
  psql -v ON_ERROR_STOP=1 --dbname "$db" -v name="$name" -v password="$password" -v db="$db" -v owner="$owner" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'name', :'password')
  WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'name') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', :'db', :'name') \gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'name') \gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA public TO %I', :'name') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT ON TABLES TO %I', :'owner', :'name') \gexec
SQL
  echo "reader $name on $db ready"
}

create_db ops "$OPS_DB_PASSWORD"
create_db tickets "$TICKETS_DB_PASSWORD"
create_db mattermost "$MATTERMOST_DB_PASSWORD"
create_db iceberg "$ICEBERG_DB_PASSWORD"

# Grafana reads the ops DB (docs/design.md §9.3).
create_reader grafana "$GRAFANA_DB_PASSWORD" ops ops
