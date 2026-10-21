#!/usr/bin/env bash
# End-to-end test for the simulated fleet and naive alerting (issue #5). Run
# it with the stack up (`make fleet-test`). It needs the fleet to have run for
# a while to see alerts, so it waits up to 15 minutes for them.
#
#  1. The fleet's jobs register in platform.jobs, and their status,
#     checkpoints and logs reach platform.* and the ops DB.
#  2. Prometheus scrapes the fleet-exporter as part of the `flink` job, and
#     every curated metric has one label set across real and simulated jobs.
#  3. The naive rules fire; alerts reach platform.alerts through Alertmanager
#     and the collector, with fleet job names and tenants resolved; most are
#     about jobs whose problems are noise.
#  4. The same rule fires for a real job's restart (F1, via cicd-sim).
set -euo pipefail

cd "$(dirname "$0")/.."

PROM=http://localhost:9090
CICD=http://localhost:8091
ROSTER=${ROSTER:-shadowtraffic/fleet/roster-local.json}

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }
step() { printf '\n%s\n' "$*"; }

retry() {
  local deadline=$((SECONDS + $1)); shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 5
  done
}

sql() { docker compose exec -T postgres psql -U ops -d ops -Atc "$1"; }
promql() { curl -sf "$PROM/api/v1/query" --data-urlencode "query=$1" | jq -r '.data.result[0].value[1] // "0"'; }

# The roster's job names, as a SQL list, by profile.
names() { jq -r --arg p "$1" '[.[] | select(.profile | test($p)) | "'"'"'" + .job_name + "'"'"'"] | join(",")' "$ROSTER"; }
fleet_size=$(jq length "$ROSTER")
all_fleet=$(names .)
genuine=$(names 'crash_loop|stalled|state_growth')

docker compose ps st-fleet --format '{{.State}}' | grep -q running || fail "st-fleet isn't running; run 'make up'"

step "Fleet events"
registered() { (($(sql "select count(*) from jobs where job_name in ($all_fleet)") >= fleet_size * 95 / 100)); }
retry 120 registered || fail "fewer than 95% of the $fleet_size fleet jobs are in the registry"
pass "$(sql "select count(*) from jobs where job_name in ($all_fleet)") of $fleet_size fleet jobs in the platform.jobs registry"
for table in job_status checkpoints logs; do
  has_events() { (($(sql "select count(distinct job_name) from $table where job_name in ($all_fleet) and ts > now() - interval '15 minutes'") >= 3)); }
  retry 120 has_events || fail "no fleet events in $table"
done
pass "fleet status, checkpoints and logs in the ops DB: $(sql "select count(*) from job_status where job_name in ($all_fleet) and ts > now() - interval '15 minutes'") status, $(sql "select count(*) from checkpoints where job_name in ($all_fleet) and ts > now() - interval '15 minutes'") checkpoints, $(sql "select count(*) from logs where job_name in ($all_fleet) and ts > now() - interval '15 minutes'") log lines in 15 minutes"

step "Metrics: real and simulated alike"
curl -sf -X POST "$CICD/baseline" -H 'Content-Type: application/json' -d '{"triggered_by": "fleet-test"}' >/dev/null ||
  fail "couldn't put the real jobs on their baselines"
scraped() { (($(promql 'count(count by (job_name) (flink_jobmanager_job_numRestarts))') >= fleet_size * 95 / 100 + 6)); }
retry 120 scraped || fail "Prometheus doesn't have numRestarts for the fleet and the real jobs"
pass "$(promql 'count(count by (job_name) (flink_jobmanager_job_numRestarts))') jobs report flink_jobmanager_job_numRestarts, from $(promql 'count(up{job="flink"} == 1)') targets in the flink scrape job"
# Each curated metric (docs/flink-metrics.md) has a single set of label
# names, shared by real and simulated series.
metrics=$(grep -oE '^\| `flink_[A-Za-z_]+`' docs/flink-metrics.md | tr -d '|` ' |
  grep -vE 'numRegisteredTaskManagers|taskSlots')
for metric in $metrics; do
  series=$(curl -sf -G "$PROM/api/v1/series" --data-urlencode "match[]=$metric" --data-urlencode "start=$(($(date +%s) - 60))")
  shapes=$(jq -r '[.data[] | keys | join(",")] | unique | length' <<<"$series")
  [[ "$shapes" == 1 ]] || fail "$metric has $shapes label sets: $(jq -c '[.data[] | keys | join(",")] | unique' <<<"$series")"
  if jq -e '.data[0] | has("job_name")' <<<"$series" >/dev/null; then
    real_series=$(jq '[.data[] | select(.job_name | IN("payments_enrichment", "market_data_to_iceberg", "audit_log_archiver"))] | length' <<<"$series")
    fleet_series=$(jq '[.data[] | select(.job_name | IN("payments_enrichment", "trade_events_ingest", "positions_aggregator", "market_data_to_iceberg", "customer_events_to_iceberg", "audit_log_archiver") | not)] | length' <<<"$series")
    ((real_series > 0 && fleet_series > 0)) || fail "$metric: $real_series real and $fleet_series simulated series"
  fi
done
pass "$(wc -w <<<"$metrics" | tr -d ' ') curated metrics: one label set each, shared by real and simulated jobs"

step "Naive alerting"
[[ "$(curl -sf "$PROM/api/v1/rules" | jq '[.data.groups[].rules[]] | length')" -ge 6 ]] || fail "alert rules not loaded"
pass "$(curl -sf "$PROM/api/v1/rules" | jq '[.data.groups[].rules[]] | length') alert rules loaded"
# Alerts since st-fleet (re)started: each start begins the genuine problems
# afresh.
started=$(docker inspect -f '{{.State.StartedAt}}' "$(docker compose ps -q st-fleet)")
window="ts > greatest('$started'::timestamptz, now() - interval '30 minutes')"
# Genuine problems fire a bounded set of alerts that stay firing; noise keeps
# coming. Judge the mix once a fair sample of noise has built up.
noisy_alerts() {
  [[ "$(sql "select count(*) >= 30 and count(distinct job_name) >= 10 from alerts where $window and status = 'firing'
             and job_name in ($all_fleet) and job_name not in ($genuine)")" == t ]]
}
retry 900 noisy_alerts || fail "fewer than 30 noise alerts from 10 fleet jobs in 15 minutes"
total=$(sql "select count(*) from alerts where $window and status = 'firing'")
noise=$(sql "select count(*) from alerts where $window and status = 'firing' and job_name in ($all_fleet) and job_name not in ($genuine)")
resolved=$(sql "select count(*) from alerts a where $window and exists (select 1 from jobs j where j.job_name = a.job_name and j.tenant = a.tenant)")
all=$(sql "select count(*) from alerts where $window")
((noise * 100 >= total * 70)) || fail "only $noise of $total firing alerts are noise"
((resolved * 100 >= all * 95)) || fail "only $resolved of $all alerts resolved to a registered job and tenant"
pass "$total alerts fired since st-fleet started, across $(sql "select count(distinct job_name) from alerts where $window") jobs; $noise ($((noise * 100 / total))%) were fleet noise"
pass "$resolved of $all alert events name a registered job and its tenant"
echo "    by rule: $(sql "select string_agg(alertname || ' ' || n, ', ' order by n desc) from (select alertname, count(*) n from alerts where $window and status = 'firing' group by 1) x")"

step "A real job alerts like a simulated one"
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
curl -sf -X POST "$CICD/deploy" -H 'Content-Type: application/json' \
  -d '{"job": "payments-enrichment", "version": "1.42.0", "triggered_by": "fleet-test"}' >/dev/null || fail "F1 deploy failed"
real_alert() { [[ "$(sql "select count(*) from alerts where job_name = 'payments-enrichment' and alertname = 'FlinkJobRestarted' and status = 'firing' and ts >= '$since'")" -ge 1 ]]; }
retry 180 real_alert || fail "no FlinkJobRestarted alert for payments-enrichment"
pass "F1 on payments-enrichment fired FlinkJobRestarted, tenant $(sql "select tenant from alerts where job_name = 'payments-enrichment' and ts >= '$since' limit 1")"
curl -sf -X POST "$CICD/rollback" -H 'Content-Type: application/json' \
  -d '{"job": "payments-enrichment", "triggered_by": "fleet-test"}' >/dev/null || fail "rollback failed"
pass "payments-enrichment rolled back"

printf '\n\033[32mFleet test passed.\033[0m\n'
