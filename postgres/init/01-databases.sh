#!/usr/bin/env bash
# Creates one database and owning role per service sharing this Postgres
# instance (docs/design.md §5.4). Runs only when the data volume is empty.
set -euo pipefail

create_db() {
  local name="$1" password="$2"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    -v name="$name" -v password="$password" <<'SQL'
CREATE ROLE :"name" LOGIN PASSWORD :'password';
CREATE DATABASE :"name" OWNER :"name";
SQL
}

create_db ops "$OPS_DB_PASSWORD"
create_db tickets "$TICKETS_DB_PASSWORD"
create_db mattermost "$MATTERMOST_DB_PASSWORD"
