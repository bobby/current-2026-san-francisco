#!/usr/bin/env bash
# Smoke test for the local platform (issue #1). Run it after `make up`.
#
# Checks that every service is healthy, then runs a throwaway Flink SQL job
# through the SQL Gateway. The job must checkpoint to S3 and report metrics
# to Prometheus. The test also creates and drops an Iceberg table through the
# REST catalog, and checks Kafka and the Postgres databases.
set -euo pipefail

cd "$(dirname "$0")/.."

FLINK=http://localhost:8081
GATEWAY=http://localhost:8083
PROM=http://localhost:9090
CATALOG=http://localhost:8181
S3=http://localhost:9000
S3_ACCESS_KEY=${S3_ACCESS_KEY:-platform}
S3_SECRET_KEY=${S3_SECRET_KEY:-platform-secret}

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; exit 1; }
step() { printf '\n%s\n' "$*"; }

# retry <seconds> <command...>: retry a command once a second until it succeeds.
retry() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 1
  done
}

s3_head() {
  curl -sf -o /dev/null -I --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "$S3_ACCESS_KEY:$S3_SECRET_KEY" "$S3/$1"
}

prom_value() {
  curl -sf "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // empty'
}

step "Services"
for svc in kafka s3 iceberg-rest jobmanager sql-gateway postgres prometheus; do
  check_health() { [[ "$(docker compose ps --format '{{.Health}}' "$svc")" == healthy ]]; }
  retry 120 check_health || fail "$svc is not healthy"
  pass "$svc healthy"
done
[[ "$(docker compose ps -a --format '{{.State}} {{.ExitCode}}' bucket-init)" == "exited 0" ]] \
  || fail "bucket-init did not complete"
pass "buckets created"

step "Flink"
tm_registered() { [[ "$(curl -sf "$FLINK/overview" | jq '.taskmanagers')" -ge 1 ]]; }
retry 60 tm_registered || fail "no TaskManager registered"
pass "$(curl -sf "$FLINK/overview" | jq -r '"\(.taskmanagers) TaskManager(s), \(."slots-total") slots, Flink \(."flink-version")"')"
scheduler=$(curl -sf "$FLINK/jobmanager/config" | jq -r '.[] | select(.key == "jobmanager.scheduler") | .value')
[[ "$scheduler" == adaptive ]] || fail "expected adaptive scheduler, got '$scheduler'"
pass "adaptive scheduler"

step "Flink SQL job through the SQL Gateway"
session=$(curl -sf -X POST "$GATEWAY/v1/sessions" -H 'Content-Type: application/json' \
  -d '{"sessionName": "smoke-test"}' | jq -r .sessionHandle)

# run_statement <sql>: execute a statement and print the first field of the first row.
run_statement() {
  local op result
  op=$(jq -n --arg s "$1" '{statement: $s}' |
    curl -sf -X POST "$GATEWAY/v1/sessions/$session/statements" \
      -H 'Content-Type: application/json' -d @- | jq -r .operationHandle)
  for _ in $(seq 60); do
    result=$(curl -sf "$GATEWAY/v1/sessions/$session/operations/$op/result/0")
    if [[ "$(jq -r .resultType <<<"$result")" != NOT_READY ]]; then
      jq -r '.results.data[0].fields[0] // empty' <<<"$result"
      return
    fi
    sleep 1
  done
  return 1
}

run_statement "CREATE TEMPORARY TABLE smoke_source (id BIGINT) WITH ('connector' = 'datagen', 'rows-per-second' = '10')" >/dev/null
run_statement "CREATE TEMPORARY TABLE smoke_sink (id BIGINT) WITH ('connector' = 'blackhole')" >/dev/null
job_id=$(run_statement "INSERT INTO smoke_sink SELECT id FROM smoke_source")
[[ -n "$job_id" ]] || fail "INSERT did not return a job id"
pass "submitted job $job_id"

cleanup_job() {
  curl -sf -X PATCH "$FLINK/jobs/$job_id?mode=cancel" >/dev/null || true
  curl -sf -X DELETE "$GATEWAY/v1/sessions/$session" >/dev/null || true
}
trap cleanup_job EXIT

checkpoint_completed() { [[ "$(curl -sf "$FLINK/jobs/$job_id/checkpoints" | jq '.counts.completed')" -ge 1 ]]; }
retry 90 checkpoint_completed || fail "no checkpoint completed"
path=$(curl -sf "$FLINK/jobs/$job_id/checkpoints" | jq -r '.latest.completed.external_path')
[[ "$path" == s3://flink/checkpoints/* ]] || fail "unexpected checkpoint path: $path"
s3_head "flink/${path#s3://flink/}/_metadata" || fail "checkpoint metadata not found in S3 at $path"
pass "checkpoint written to $path"

checkpoint_metric() { [[ -n "$(prom_value "flink_jobmanager_job_numberOfCompletedCheckpoints{job_id=\"$job_id\"}")" ]]; }
retry 60 checkpoint_metric || fail "job metrics not in Prometheus"
pass "job metrics in Prometheus"

cleanup_job
trap - EXIT
pass "job cancelled"

step "Prometheus"
for target in $(curl -sf "$PROM/api/v1/targets" | jq -r '.data.activeTargets[] | select(.labels.job == "flink") | "\(.scrapeUrl)=\(.health)"'); do
  [[ "$target" == *=up ]] || fail "Flink scrape target down: $target"
done
pass "all Flink scrape targets up"
tms=$(prom_value 'flink_jobmanager_numRegisteredTaskManagers')
[[ "${tms:-0}" -ge 1 ]] || fail "flink_jobmanager_numRegisteredTaskManagers missing"
pass "flink_jobmanager_numRegisteredTaskManagers = $tms"

step "Iceberg REST catalog"
curl -sf -X DELETE "$CATALOG/v1/namespaces/smoke/tables/t?purgeRequested=true" >/dev/null || true
curl -sf -X DELETE "$CATALOG/v1/namespaces/smoke" >/dev/null || true
curl -sf -X POST "$CATALOG/v1/namespaces" -H 'Content-Type: application/json' \
  -d '{"namespace": ["smoke"]}' >/dev/null || fail "create namespace failed"
metadata=$(curl -sf -X POST "$CATALOG/v1/namespaces/smoke/tables" -H 'Content-Type: application/json' \
  -d '{"name": "t", "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "required": false, "type": "long"}]}}' |
  jq -r '."metadata-location"') || fail "create table failed"
s3_head "warehouse/${metadata#s3://warehouse/}" || fail "table metadata not found in S3 at $metadata"
pass "table metadata written to $metadata"
curl -sf -X DELETE "$CATALOG/v1/namespaces/smoke/tables/t?purgeRequested=true" >/dev/null
curl -sf -X DELETE "$CATALOG/v1/namespaces/smoke" >/dev/null
pass "table and namespace dropped"

step "Kafka"
kafka() { local cmd=$1; shift; docker compose exec -T kafka "/opt/kafka/bin/$cmd" "$@"; }
kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic smoke-test >/dev/null
echo hello | kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic smoke-test
msg=$(kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic smoke-test \
  --from-beginning --max-messages 1 --timeout-ms 15000 2>/dev/null)
[[ "$msg" == hello ]] || fail "Kafka round trip failed"
kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic smoke-test
pass "produce/consume round trip"

step "Postgres"
for db in ops tickets mattermost iceberg; do
  var="$(tr '[:lower:]' '[:upper:]' <<<"$db")_DB_PASSWORD"
  docker compose exec -T -e PGPASSWORD="${!var:-$db}" postgres \
    psql -h localhost -U "$db" -d "$db" -tAc 'select 1' >/dev/null || fail "cannot connect to $db as $db"
  pass "database $db"
done

step "Memory"
docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}' $(docker compose ps -q) |
  sed 's/^platform-//' | column -t -s $'\t' | sed 's/^/  /'

printf '\n\033[32mSmoke test passed.\033[0m\n'
