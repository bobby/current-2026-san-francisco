#!/usr/bin/env bash
# End-to-end test for the platform's jobs (issue #2). Run it with the stack up
# and the jar built (`make jobs-test` does both checks).
#
#  1. Every job runs healthy and commits records to its Iceberg table.
#  2. Each fault fails as designed (docs/design.md §7.1):
#     - F1 --fault=npe-on-null-field: NullPointerException restart loop
#     - F2 malformed record with --on-deser-error=fail: restart loop naming
#       the record's offset; with --on-deser-error=dlq: diverted to
#       trade-events.dlq and the job stays healthy
#     - F4 --fault=state-leak: checkpointed state grows by kilobytes per fill,
#       versus bytes when healthy
#  3. Every job is left running healthy.
#
# The test produces its own fixture records, so it works with or without
# ShadowTraffic (st-inputs) running.
set -euo pipefail

cd "$(dirname "$0")/.."

FLINK=http://localhost:8081
CATALOG=http://localhost:8181
JOBS=scripts/jobs.sh

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; exit 1; }
step() { printf '\n%s\n' "$*"; }

# retry <seconds> <command...>: retry a command every 2 s until it succeeds.
retry() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

# produce <topic>: write JSON lines from stdin to a topic.
produce() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$1" 2>/dev/null
}

job_id() {
  curl -sf "$FLINK/jobs/overview" |
    jq -r --arg name "$1" '.jobs[] | select(.name == $name and .state == "RUNNING") | .jid' | head -1
}

# submit <job> [flags...]: (re)deploy a job and wait until all its tasks run.
submit() {
  local job=$1
  $JOBS cancel "$job" >/dev/null 2>&1 || true
  retry 60 no_active_job "$job" || fail "$job did not cancel"
  $JOBS submit "$@" >/dev/null
  retry 90 tasks_running "$job" || fail "$job did not start"
}

no_active_job() {
  ! curl -sf "$FLINK/jobs/overview" |
    jq -e --arg name "$1" '.jobs[] | select(.name == $name and (.state | IN("FINISHED", "CANCELED", "FAILED") | not))' >/dev/null
}

tasks_running() {
  local jid; jid=$(job_id "$1")
  [[ -n "$jid" ]] && curl -sf "$FLINK/jobs/$jid" | jq -e '[.vertices[].status] | all(. == "RUNNING")' >/dev/null
}

# exception_seen <job> <pattern>: the job's latest exception matches pattern.
exception_seen() {
  local jid; jid=$(curl -sf "$FLINK/jobs/overview" |
    jq -r --arg name "$1" '.jobs[] | select(.name == $name and (.state | IN("FINISHED", "CANCELED", "FAILED") | not)) | .jid' | head -1)
  [[ -n "$jid" ]] && curl -sf "$FLINK/jobs/$jid/exceptions" |
    jq -e --arg p "$2" '[.exceptionHistory.entries[] | .exceptionName + " " + .stacktrace] | any(test($p))' >/dev/null
}

table_records() {
  curl -sf "$CATALOG/v1/namespaces/$1/tables/$2" |
    jq -r '.metadata.snapshots[-1].summary["total-records"] // "0"' 2>/dev/null || echo 0
}

# checkpoint_after <job> <timestamp ms>: latest completed checkpoint's state
# size, once a checkpoint triggered after the timestamp has completed.
checkpoint_after() {
  local jid; jid=$(job_id "$1")
  curl -sf "$FLINK/jobs/$jid/checkpoints" |
    jq -e --argjson t "$2" '.latest.completed | select(.trigger_timestamp > $t) | .state_size'
}

now_ms() { echo $(($(date +%s) * 1000)); }

# Fixture records, shaped like shadowtraffic/inputs.json.
payments() {  # <count>; every 5th merchant has no category
  jq -nc --argjson n "$1" --arg run "$RANDOM" 'range($n) | {
    payment_id: "test-\($run)-\(.)", account_id: "ACC-1001", amount: 42.5, currency: "USD",
    merchant: {id: "MER-10001", name: "Test Merchant", category: (if . % 5 == 0 then null else "grocery" end)},
    channel: "ach", created_at: (now * 1000 | floor)}'
}
trades() {
  jq -nc --argjson n "$1" --arg run "$RANDOM" 'range($n) | {
    trade_id: "test-\($run)-\(.)", account_id: "ACC-1001", instrument: "AAPL", side: "BUY",
    quantity: 10, price: 190.25, venue: "XNYS", executed_at: (now * 1000 | floor)}'
}
fills() {
  jq -nc --argjson n "$1" --arg run "$RANDOM" 'range($n) | {
    fill_id: "test-\($run)-\(.)", account_id: "ACC-\(1000 + . % 50)", instrument: "MSFT",
    side: (if . % 2 == 0 then "BUY" else "SELL" end), quantity: 10, price: 410.5, filled_at: (now * 1000 | floor)}'
}
quotes() {
  jq -nc --argjson n "$1" 'range($n) | {symbol: "AAPL", venue: "XNAS", bid: 190.1, ask: 190.2, last: 190.15, size: 100, ts: (now * 1000 | floor)}'
}
customer_events() {
  jq -nc --argjson n "$1" --arg run "$RANDOM" 'range($n) | {event_id: "test-\($run)-\(.)", customer_id: "CUST-100001",
    event_type: "login", channel: "web", session_id: "s-\($run)", ts: (now * 1000 | floor)}'
}
audit_entries() {
  jq -nc --argjson n "$1" --arg run "$RANDOM" 'range($n) | {audit_id: "test-\($run)-\(.)", actor: "alice", action: "read",
    resource: "test", outcome: "allowed", source_ip: "10.0.0.1", ts: (now * 1000 | floor)}'
}

[[ -f jobs/java/target/platform-jobs.jar ]] || fail "jar not built; run 'make jobs'"
curl -sf "$FLINK/overview" >/dev/null || fail "Flink isn't reachable; run 'make up'"

step "Every job runs and commits to Iceberg"
TABLES=(
  "payments-enrichment payments payments payments_eng payments_enriched"
  "trade-events-ingest trade-events trades trading_data trade_events"
  "positions-aggregator trade-fills fills risk_platform positions"
  "market-data-to-iceberg market-data quotes market_data quotes"
  "customer-events-to-iceberg customer-events customer_events customer_360 customer_events"
  "audit-log-archiver audit-log audit_entries compliance_eng audit_log"
)
for row in "${TABLES[@]}"; do
  read -r job topic fixture ns table <<<"$row"
  submit "$job"
  before=$(table_records "$ns" "$table")
  "$fixture" 50 | produce "$topic"
  grew() { (($(table_records "$ns" "$table") >= before + 50)); }
  retry 90 grew || fail "$job: no new records committed to $ns.$table"
  pass "$job → lake.$ns.$table ($(table_records "$ns" "$table") records)"
done

step "F1: --fault=npe-on-null-field"
submit payments-enrichment --fault=npe-on-null-field
payments 20 | produce payments
retry 90 exception_seen payments-enrichment NullPointerException || fail "no NullPointerException"
pass "NullPointerException on a payment with no merchant category"
restarts() { [[ "$(curl -sf "$FLINK/jobs/$(curl -sf "$FLINK/jobs/overview" | jq -r '.jobs[] | select(.name == "payments-enrichment" and .state != "CANCELED") | .jid' | head -1)/exceptions" | jq '.exceptionHistory.entries | length')" -ge 3 ]]; }
retry 90 restarts || fail "job is not restart-looping"
pass "restart loop (3+ failures)"
submit payments-enrichment
pass "healthy version redeployed"

step "F2: malformed record on trade-events"
poison="poison-$RANDOM$RANDOM"
# "12O" (letter O) where an integer quantity belongs.
jq -nc --arg id "$poison" '{trade_id: $id, account_id: "ACC-1001", instrument: "AAPL", side: "BUY",
  quantity: "12O", price: 190.25, venue: "XNYS", executed_at: 0}' | produce trade-events
retry 90 exception_seen trade-events-ingest 'MalformedRecordException.*trade-events partition [0-9]+ at offset [0-9]+' ||
  fail "no MalformedRecordException naming the offset"
pass "--on-deser-error=fail: MalformedRecordException naming topic, partition and offset"
submit trade-events-ingest --on-deser-error=dlq
dlq_has_poison() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic trade-events.dlq --from-beginning --timeout-ms 5000 2>/dev/null | grep -q "$poison"
}
retry 60 dlq_has_poison || fail "poison record not in trade-events.dlq"
pass "--on-deser-error=dlq: poison record diverted to trade-events.dlq"
# Kafka offsets are committed asynchronously once a checkpoint completes, so
# wait for a second checkpoint to be sure the first one's commit has landed.
for _ in 1 2; do
  t=$(now_ms)
  retry 60 checkpoint_after trade-events-ingest "$t" || fail "no checkpoint after diverting"
done
exception_seen trade-events-ingest MalformedRecordException && fail "dlq mode still failing"
pass "job healthy; checkpoint committed past the poison record"
submit trade-events-ingest
pass "baseline (fail mode) redeployed"

step "F4: --fault=state-leak"
# growth_per_fill [flags...]: bytes of checkpointed state added per fill.
growth_per_fill() {
  submit positions-aggregator "$@"
  local t s1 s2
  fills 500 | produce trade-fills
  t=$(now_ms); sleep 3
  retry 60 checkpoint_after positions-aggregator "$t" || fail "no checkpoint"
  s1=$(checkpoint_after positions-aggregator "$t")
  fills 2000 | produce trade-fills
  t=$(now_ms); sleep 3
  retry 60 checkpoint_after positions-aggregator "$t" || fail "no checkpoint"
  s2=$(checkpoint_after positions-aggregator "$t")
  echo $(((s2 - s1) / 2000))
}
healthy=$(growth_per_fill)
leaky=$(growth_per_fill --fault=state-leak)
((leaky >= 2000)) || fail "leaky state grew only $leaky bytes/fill"
((leaky >= 10 * (healthy > 0 ? healthy : 1))) || fail "leaky ($leaky B/fill) not clearly above healthy ($healthy B/fill)"
pass "state grows $leaky bytes/fill with the fault, vs $healthy bytes/fill healthy"
submit positions-aggregator
pass "healthy version redeployed"

step "All jobs healthy"
for row in "${TABLES[@]}"; do
  read -r job _ <<<"$row"
  tasks_running "$job" || fail "$job is not running"
done
pass "$($JOBS list | wc -l | tr -d ' ') jobs running"

printf '\n\033[32mJobs test passed.\033[0m\n'
