#!/usr/bin/env bash
# End-to-end test for the telemetry plane (issue #4). Run it with the stack up
# and the jobs jar built (`make telemetry-test` does both checks).
#
#  1. A healthy job's status and checkpoints reach platform.job-status and
#     platform.checkpoints, and the ops DB.
#  2. A restart loop (F1, --fault=npe-on-null-field) reaches the same topics
#     with its root cause, and its logs reach platform.logs via Vector.
#  3. Alertmanager webhooks become platform.alerts events, once per
#     transition, with the platform job name and tenant resolved.
#  4. Registry and deployment events reach the ops DB.
#  5. Point-in-time queries: checkpoint history before a time, and a job's
#     state as of a time.
#
# It leaves payments-enrichment and audit-log-archiver running healthy.
set -euo pipefail

cd "$(dirname "$0")/.."

FLINK=http://localhost:8081
COLLECTOR=http://localhost:8090
JOBS=scripts/jobs.sh

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; exit 1; }
step() { printf '\n%s\n' "$*"; }

retry() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 2
  done
}

sql() { docker compose exec -T postgres psql -U ops -d ops -Atc "$1"; }

# topic_has <topic> <pattern>: some record on the topic matches the pattern.
topic_has() {
  # Busy topics never go idle long enough for --timeout-ms, so bound the read
  # with `timeout` and stop at the first match. (Process substitution keeps
  # the consumer's SIGPIPE from failing the test under pipefail.)
  grep -q -m1 -- "$2" < <(docker compose exec -T kafka timeout 30 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic "$1" --from-beginning 2>/dev/null)
}

produce() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic "$1" --reader-property parse.key=true --reader-property key.separator=$'\t' 2>/dev/null
}

# deploy <job> [flags...]: (re)deploys a job and prints its Flink job ID.
deploy() {
  $JOBS cancel "$1" >/dev/null 2>&1 || true
  $JOBS submit "$@" | awk '{print $2}'
}

alert() {  # <status> <starts_at> [ends_at]
  jq -n --arg status "$1" --arg starts "$2" --arg ends "${3:-0001-01-01T00:00:00Z}" '{
    version: "4", status: $status, receiver: "collector", alerts: [{
      status: $status, fingerprint: "telemetry-test-\($starts)",
      labels: {alertname: "FlinkJobRestarting", severity: "warning", job_name: "payments_enrichment"},
      annotations: {summary: "payments-enrichment is restarting"},
      startsAt: $starts, endsAt: $ends, generatorURL: "http://prometheus:9090/graph"}]}' |
    curl -sf -X POST "$COLLECTOR/alertmanager" -H 'Content-Type: application/json' -d @-
}

curl -sf "$COLLECTOR/health" >/dev/null || fail "the collector isn't reachable; run 'make up'"
[[ -f jobs/java/target/platform-jobs.jar ]] || fail "jar not built; run 'make jobs'"

step "Topics"
for topic in platform.{jobs,deployments,job-status,checkpoints,logs,alerts,signals,actions} fleet.telemetry; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic "$topic" >/dev/null 2>&1 || fail "$topic is missing"
done
jobs_topic=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic platform.jobs 2>/dev/null)
grep -q cleanup.policy=compact <<<"$jobs_topic" || fail "platform.jobs isn't compacted"
pass "all eight platform.* topics and fleet.telemetry exist; platform.jobs is compacted"

step "A healthy job: status and checkpoints"
audit=$(deploy audit-log-archiver)
retry 60 sql "select 1 from job_status where flink_job_id = '$audit' and status = 'RUNNING'" ||
  fail "no RUNNING status for audit-log-archiver"
topic_has platform.job-status "\"flink_job_id\":\"$audit\"" || fail "status not on platform.job-status"
pass "audit-log-archiver RUNNING in platform.job-status and the ops DB"
three_checkpoints() { (($(sql "select count(*) from checkpoints where flink_job_id = '$audit' and status = 'completed'") >= 3)); }
retry 90 three_checkpoints || fail "fewer than 3 checkpoints recorded"
topic_has platform.checkpoints "\"flink_job_id\":\"$audit\"" || fail "checkpoints not on platform.checkpoints"
pass "checkpoints in platform.checkpoints and the ops DB"

step "F1 restart loop: status, root cause and logs"
payments=$(deploy payments-enrichment --fault=npe-on-null-field)
restarted() {
  (($(sql "select count(*) from job_status where flink_job_id = '$payments' and status = 'RESTARTING'
           and root_cause_class = 'java.lang.NullPointerException'") >= 2))
}
retry 120 restarted || fail "no restart loop with a NullPointerException root cause"
topic_has platform.job-status "\"root_cause_class\":\"java.lang.NullPointerException\"" ||
  fail "restart not on platform.job-status"
pass "restarts in platform.job-status with root cause NullPointerException ($(sql "select max(restarts) from job_status where flink_job_id = '$payments'") so far)"
logged() { sql "select 1 from logs where flink_job_id = '$payments' and root_cause_class = 'java.lang.NullPointerException'" | grep -q 1; }
retry 60 logged || fail "no NullPointerException logs in the ops DB"
topic_has platform.logs "\"flink_job_id\":\"$payments\"" || fail "logs not on platform.logs"
pass "Flink logs in platform.logs with the job name: $(sql "select count(*) from logs where flink_job_id = '$payments'") lines"

step "Alertmanager webhook"
starts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
alert firing "$starts"
alert firing "$starts"  # Alertmanager repeats notifications.
fingerprint="telemetry-test-$starts"
firing() { [[ "$(sql "select count(*) || ' ' || min(job_name) from alerts where fingerprint = '$fingerprint'")" == "1 payments-enrichment" ]]; }
retry 30 firing || fail "firing alert not stored once with job payments-enrichment: $(sql "select count(*), min(job_name) from alerts where fingerprint = '$fingerprint'")"
pass "firing alert stored once, job_name payments_enrichment resolved to payments-enrichment"
alert resolved "$starts" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
resolved() { [[ "$(sql "select string_agg(status, ',' order by ts) from alerts where fingerprint = '$fingerprint'")" == "firing,resolved" ]]; }
retry 30 resolved || fail "resolved alert not stored"
topic_has platform.alerts "\"fingerprint\":\"$fingerprint\"" || fail "alerts not on platform.alerts"
pass "resolved alert stored; both in platform.alerts"

step "Job registry and deployments"
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
deployment_id=$(uuidgen | tr 'A-Z' 'a-z')
{
  printf 'payments-enrichment\t%s\n' "$(jq -nc --arg now "$now" --arg jid "$payments" '{
    job_name: "payments-enrichment", tenant: "payments-eng", owner: "@payments-eng", tier: "tier1", kind: "java",
    current_version: "v8", flink_job_id: $jid, max_parallelism: 4, deployed_at: $now, first_deployed_at: "2026-10-01T09:00:00Z"}')"
} | produce platform.jobs
{
  printf 'payments-enrichment\t%s\n' "$(jq -nc --arg now "$now" --arg id "$deployment_id" --arg jid "$payments" '{
    event_id: $id, ts: $now, job_name: "payments-enrichment", tenant: "payments-eng", kind: "deploy",
    from_version: "v7", to_version: "v8", commit_sha: "9f2c1ab", author: "dana", summary: "Normalise merchant categories",
    args: ["--job=payments-enrichment"], triggered_by: "telemetry-test", outcome: "succeeded", failure_reason: null,
    flink_job_id: $jid, restored_from: null}')"
} | produce platform.deployments
registered() { [[ "$(sql "select tenant || ' ' || current_version from jobs where job_name = 'payments-enrichment'")" == "payments-eng v8" ]]; }
retry 30 registered || fail "job registry not in the ops DB"
deployed() { sql "select 1 from deployments where event_id = '$deployment_id'" | grep -q 1; }
retry 30 deployed || fail "deployment not in the ops DB"
pass "platform.jobs and platform.deployments in the ops DB"
starts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
alert firing "$starts"
tenant() { [[ "$(sql "select tenant from alerts where fingerprint = 'telemetry-test-$starts'")" == payments-eng ]]; }
retry 30 tenant || fail "alert tenant not resolved from the registry"
pass "alerts without a tenant label get the tenant from the registry"

step "Point-in-time queries"
# The time of audit-log-archiver's second completed checkpoint.
cutoff=$(sql "select ts from checkpoints where flink_job_id = '$audit' and status = 'completed' order by checkpoint_id offset 1 limit 1")
before=$(sql "select string_agg(checkpoint_id::text, ',' order by ts desc) from checkpoints
              where job_name = 'audit-log-archiver' and flink_job_id = '$audit' and ts < '$cutoff'")
[[ "$before" == 1 ]] || fail "checkpoint history before $cutoff: expected [1], got [$before]"
pass "checkpoint history for audit-log-archiver before $cutoff: checkpoint $before"
# payments-enrichment's state at the moment the alert fired: restarting, with
# the deploy that preceded it.
snapshot=$(sql "select s.status || ' ' || s.root_cause_class || ' after ' || coalesce(d.to_version, 'no deploy')
  from (select * from job_status where job_name = 'payments-enrichment' and ts <= '$starts' order by ts desc limit 1) s
  left join lateral (select * from deployments where job_name = 'payments-enrichment' and ts <= '$starts'
                     order by ts desc limit 1) d on true")
[[ "$snapshot" == "RESTARTING java.lang.NullPointerException after v8" ]] || fail "job state at $starts: $snapshot"
pass "payments-enrichment as of $starts: $snapshot"

step "Clean up"
deploy payments-enrichment >/dev/null
pass "payments-enrichment redeployed healthy"

printf '\n\033[32mTelemetry test passed.\033[0m\n'
