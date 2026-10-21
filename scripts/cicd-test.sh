#!/usr/bin/env bash
# End-to-end test for cicd-sim (issue #6). Run it with the stack up and the
# jobs jar built (`make cicd-test` does both checks). Takes a few minutes.
#
#  1. Baseline: every job runs its catalogue baseline and is in the registry.
#  2. A SQL job deploy stops the old version with a savepoint and restores
#     the new one from it.
#  3. F1: deploying the faulty payments-enrichment 1.42.0 restart-loops;
#     rolling back restores 1.41.0 from the newest retained savepoint or
#     checkpoint (the looping run can't take a savepoint), and it recovers.
#  4. A deploy can add job arguments (the F2 fix, --on-deser-error=dlq).
#  5. Deployments, the registry and the rollback's action reach their topics
#     and the ops DB.
#
# It leaves every job on its baseline version.
set -euo pipefail

cd "$(dirname "$0")/.."

FLINK=http://localhost:8081
CICD=http://localhost:8091
CATALOG=http://localhost:8181
export TRIGGERED_BY=cicd-test

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }
step() { printf '\n%s\n' "$*"; }

retry() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

sql() { docker compose exec -T postgres psql -U ops -d ops -Atc "$1"; }

topic_has() {
  # Busy topics never go idle long enough for --timeout-ms, so bound the read
  # with `timeout` and stop at the first match. (Process substitution keeps
  # the consumer's SIGPIPE from failing the test under pipefail.)
  grep -q -m1 -- "$2" < <(docker compose exec -T kafka timeout 30 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$1" --from-beginning 2>/dev/null)
}

# cicd <path> <json>: POST to cicd-sim; prints the body, fails on non-200.
cicd() {
  local response status
  response=$(curl -s -w '\n%{http_code}' -X POST "$CICD$1" -H 'Content-Type: application/json' -d "$2")
  status=${response##*$'\n'}
  [[ "$status" == 200 ]] || fail "POST $1 answered $status: ${response%$'\n'*}"
  printf '%s' "${response%$'\n'*}"
}

deploy() {  # <job> <version> [args...]
  cicd /deploy "$(printf '%s\n' "${@:3}" | jq -R 'select(length > 0)' | jq -s --arg job "$1" --arg v "$2" \
    '{job: $job, version: $v, args: ., triggered_by: "cicd-test"}')"
}

checkpoints() {  # <jid> <n>: the run has completed at least n checkpoints
  curl -sf "$FLINK/jobs/$1/checkpoints" | jq -e --argjson n "$2" '.counts.completed >= $n'
}

restarts() { curl -sf "$FLINK/jobs/$1/metrics?get=numRestarts" | jq -r '.[0].value // "0"'; }

table_records() {
  curl -sf "$CATALOG/v1/namespaces/$1/tables/$2" | jq -r '.metadata.snapshots[-1].summary["total-records"] // "0"'
}

curl -sf "$CICD/health" >/dev/null || fail "cicd-sim isn't reachable; run 'make up'"
[[ -f jobs/java/target/platform-jobs.jar ]] || fail "jar not built; run 'make jobs'"

step "Catalogue and baseline"
status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$CICD/deploy" -H 'Content-Type: application/json' \
  -d '{"job": "payments-enrichment", "version": "9.9.9"}')
[[ "$status" == 404 ]] || fail "an unknown version answered $status, not 404"
pass "unknown versions are rejected (404)"
cicd /baseline '{"triggered_by": "cicd-test"}' >/dev/null
baselines=$(curl -sf "$CICD/catalogue" | jq -r '.jobs | to_entries[] | "\(.key) \(.value.baseline)"')
on_baseline() {
  local job version
  while read -r job version; do
    [[ "$(sql "select current_version from jobs where job_name = '$job'")" == "$version" ]] || return 1
    curl -sf "$FLINK/jobs/overview" | jq -e --arg n "$job" 'any(.jobs[]; .name == $n and .state == "RUNNING")' >/dev/null || return 1
  done <<<"$baselines"
}
retry 60 on_baseline || fail "not every job is running its baseline version"
pass "all $(wc -l <<<"$baselines" | tr -d ' ') jobs run their baseline versions and are in the registry"

step "SQL job deploy with a savepoint"
d=$(deploy audit-log-archiver 3.31.0)
restored=$(jq -r .restored_from <<<"$d")
[[ "$restored" == s3://*/savepoints/* ]] || fail "audit-log-archiver 3.31.0 didn't restore from a savepoint: $restored"
[[ "$(jq -r '.from_version + " → " + .to_version' <<<"$d")" == "3.30.0 → 3.31.0" ]] || fail "wrong versions: $d"
audit=$(jq -r .flink_job_id <<<"$d")
retry 60 checkpoints "$audit" 1 || fail "audit-log-archiver 3.31.0 isn't checkpointing"
pass "3.30.0 → 3.31.0 restored from $restored"

step "F1: faulty deploy and rollback"
d=$(deploy payments-enrichment 1.42.0)
faulty=$(jq -r .flink_job_id <<<"$d")
[[ "$(jq -r .restored_from <<<"$d")" == s3://*/savepoints/* ]] || fail "1.42.0 didn't restore from the 1.41.0 savepoint"
pass "1.41.0 → 1.42.0 deployed from a savepoint (args: $(jq -r '.args | join(" ")' <<<"$d"))"
looping() {
  (($(sql "select count(*) from job_status where flink_job_id = '$faulty' and status = 'RESTARTING'
           and root_cause_class = 'java.lang.NullPointerException'") >= 3))
}
retry 120 looping || fail "1.42.0 isn't restart-looping on a NullPointerException"
pass "1.42.0 is restart-looping on a NullPointerException"
before=$(table_records payments_eng payments_enriched)
action_id=$(uuidgen | tr 'A-Z' 'a-z')
d=$(cicd /rollback "$(jq -n --arg a "$action_id" '{job: "payments-enrichment", triggered_by: "operator", action_id: $a}')")
[[ "$(jq -r '.kind + " " + .from_version + " → " + .to_version' <<<"$d")" == "rollback 1.42.0 → 1.41.0" ]] ||
  fail "unexpected rollback: $d"
restored=$(jq -r .restored_from <<<"$d")
[[ "$restored" == s3://* ]] || fail "rollback didn't restore from a retained savepoint or checkpoint"
rolled_back=$(jq -r .flink_job_id <<<"$d")
pass "rolled back 1.42.0 → 1.41.0 from $restored"
retry 90 checkpoints "$rolled_back" 3 || fail "1.41.0 isn't checkpointing after the rollback"
[[ "$(restarts "$rolled_back")" == 0 ]] || fail "1.41.0 restarted after the rollback"
grew() { (($(table_records payments_eng payments_enriched) > before)); }
retry 60 grew || fail "no new records committed after the rollback"
pass "1.41.0 healthy after the rollback: 3 checkpoints, 0 restarts, records committed"

step "Deploy with extra args (the F2 fix)"
d=$(deploy trade-events-ingest 2.18.0 --on-deser-error=dlq)
[[ "$(jq -r '.args | join(" ")' <<<"$d")" == "--job=trade-events-ingest --on-deser-error=dlq" ]] || fail "args not recorded: $d"
pass "2.18.0 deployed with --on-deser-error=dlq"

step "Events and the ops DB"
deployed() { [[ "$(sql "select count(*) from deployments where triggered_by in ('cicd-test', 'operator') and ts > now() - interval '15 minutes'")" -ge 4 ]]; }
retry 30 deployed || fail "deployments missing from the ops DB"
topic_has platform.deployments "\"flink_job_id\":\"$rolled_back\"" || fail "rollback not on platform.deployments"
pass "deployments in platform.deployments and the ops DB"
registered() { [[ "$(sql "select current_version || ' ' || flink_job_id from jobs where job_name = 'payments-enrichment'")" == "1.41.0 $rolled_back" ]]; }
retry 30 registered || fail "registry doesn't show payments-enrichment 1.41.0"
topic_has platform.jobs "\"flink_job_id\":\"$rolled_back\"" || fail "registry update not on platform.jobs"
pass "platform.jobs registry: payments-enrichment on 1.41.0 ($rolled_back)"
acted() { [[ "$(sql "select phase from actions where action_id = '$action_id'")" == executed ]]; }
retry 30 acted || fail "rollback action not recorded"
pass "rollback recorded in platform.actions as executed"

step "Back to baseline"
cicd /baseline '{"triggered_by": "cicd-test"}' >/dev/null
retry 60 on_baseline || fail "not every job is back on its baseline version"
pass "every job on its baseline version"

printf '\n\033[32mcicd-sim test passed.\033[0m\n'
