#!/usr/bin/env bash
# The operator agent's done-when check (issue #10): on the scripted timeline,
# F1 is rolled back automatically and its ticket resolved, F3 is rescaled
# and its lag drains, and F4 is escalated with evidence. Needs the stack up
# and JEV_API_KEY in .env.
#
#   scripts/operator-test.sh            reset, run the whole timeline (~40 min), check
#   scripts/operator-test.sh --no-run   check the latest run again
#
# Checks:
#  1. F1: the operator rolled payments-enrichment back through cicd-sim,
#     verified the recovery, and resolved the ticket.
#  2. F3: it rescaled market-data-to-iceberg within the tenant's maximum,
#     and lag fell between the action and the verification.
#  3. F4: it escalated positions-aggregator to the owning team, with the
#     ticket assigned to them and a card offering Acknowledge and the
#     temporary restart. Clicking the restart, through Mattermost, restarts
#     the job with its state on disk.
#  4. Guardrails: no automatic action on any other job; every incident has
#     a ticket, a card and an audit trail in platform.actions.
set -euo pipefail

cd "$(dirname "$0")/.."

MM=http://localhost:8065/api/v4
OPERATOR=http://localhost:8095
TICKETS=http://localhost:8094
PROMETHEUS=http://localhost:9090
ADMIN_USER=oncall
ADMIN_PASSWORD=${MATTERMOST_ADMIN_PASSWORD:-oncall-demo-2026}

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }
step() { printf '\n%s\n' "$*"; }
sql() { docker compose exec -T postgres psql -U ops -d ops -Atc "$1"; }
tickets() { curl -sf -u operator-test: -H 'Accept: application/json' -G "$TICKETS/api/now/table/incident" --data-urlencode "sysparm_query=$1"; }

[[ $(curl -sf "$OPERATOR/stats" | jq -r .enabled) == true ]] ||
  fail "the operator agent isn't operating; set JEV_API_KEY in .env and run 'make up'"

if [[ ${1:-} != --no-run ]]; then
  scripts/director.sh reset
  scripts/director.sh run --scenario all
fi

started=$(jq -r '.run.started_at // empty' .director/state.json)
[[ -n "$started" ]] || fail "no director run to check"

step "Waiting for the operator's incidents to finish (up to 10 minutes)"
for _ in $(seq 120); do
  open=$(curl -sf "$OPERATOR/incidents" | jq '[.[] | select(.stage | IN("resolved", "escalated", "failed") | not)] | length')
  ((open == 0)) && break
  sleep 5
done
((open == 0)) || fail "$open incidents still in progress: $(curl -sf "$OPERATOR/incidents" | jq -c '[.[] | select(.stage | IN("resolved", "escalated", "failed") | not) | {job_name, stage}]')"
incidents=$(curl -sf "$OPERATOR/incidents")
pass "$(jq length <<<"$incidents") incidents: $(jq -r 'group_by(.stage) | map("\(length) \(.[0].stage)") | join(", ")' <<<"$incidents")"
failed=$(jq -r '[.[] | select(.stage == "failed") | "\(.job_name): \(.outcome)"] | join("; ")' <<<"$incidents")
[[ -z "$failed" ]] || fail "incidents that failed: $failed"

incident() { jq -c --arg j "$1" '[.[] | select(.job_name == $j)] | sort_by(.opened_at) | first // empty' <<<"$incidents"; }
since() { sql "SELECT to_char($1 - '$started'::timestamptz, 'MI:SS')"; }

step "F1: rolled back automatically, ticket resolved"
f1=$(incident payments-enrichment)
[[ -n "$f1" ]] || fail "no incident for payments-enrichment"
[[ $(jq -r .stage <<<"$f1") == resolved ]] || fail "payments-enrichment: $(jq -c '{stage, remediation, outcome}' <<<"$f1")"
id=$(jq -r .incident_id <<<"$f1")
rollback=$(sql "SELECT from_version || ' → ' || to_version || ' at ' || to_char(ts - '$started'::timestamptz, 'MI:SS')
                FROM deployments WHERE job_name = 'payments-enrichment' AND kind = 'rollback'
                AND triggered_by = 'operator' AND outcome = 'succeeded' AND ts >= '$started' ORDER BY ts LIMIT 1")
[[ -n "$rollback" ]] || fail "no operator rollback of payments-enrichment in platform.deployments"
pass "rolled back $rollback (signal → incident at $(since "'$(jq -r .opened_at <<<"$f1")'::timestamptz"))"
verified=$(sql "SELECT to_char(ts - '$started'::timestamptz, 'MI:SS') FROM actions
                WHERE incident_id = '$id' AND phase = 'verified' AND actor = 'operator' LIMIT 1")
[[ -n "$verified" ]] || fail "the rollback wasn't verified"
ticket=$(tickets "correlation_id=$id" | jq -c '.result[0] // empty')
[[ $(jq -r .state <<<"$ticket") == 6 ]] || fail "ticket $(jq -r .number <<<"$ticket") isn't resolved: state $(jq -r .state <<<"$ticket")"
pass "recovery verified at $verified; $(jq -r .number <<<"$ticket") resolved ($(jq -r .close_code <<<"$ticket"))"

step "F3: rescaled automatically, lag drained"
f3=$(incident market-data-to-iceberg)
[[ -n "$f3" ]] || fail "no incident for market-data-to-iceberg"
[[ $(jq -r .stage <<<"$f3") == resolved ]] || fail "market-data-to-iceberg: $(jq -c '{stage, remediation, outcome}' <<<"$f3")"
id=$(jq -r .incident_id <<<"$f3")
read -r scaled_at to_parallelism < <(sql "SELECT extract(epoch FROM ts)::bigint, event->'params'->>'to_parallelism' FROM actions
  WHERE incident_id = '$id' AND action = 'scale_up' AND phase = 'executed' AND actor = 'operator' LIMIT 1" | tr '|' ' ')
[[ -n "${scaled_at:-}" ]] || fail "no executed scale-up for market-data-to-iceberg"
max=$(curl -sf localhost:8091/catalogue | jq -r '.jobs["market-data-to-iceberg"].max_parallelism')
((to_parallelism <= max)) || fail "scaled to $to_parallelism, over the tenant's maximum $max"
verified_at=$(sql "SELECT extract(epoch FROM ts)::bigint FROM actions WHERE incident_id = '$id' AND phase = 'verified' LIMIT 1")
[[ -n "$verified_at" ]] || fail "the rescale wasn't verified"
lag() { curl -sf "$PROMETHEUS/api/v1/query" --data-urlencode \
  'query=sum(flink_taskmanager_job_task_operator_pendingRecords{job_name="market_data_to_iceberg"})' \
  --data-urlencode "time=$1" | jq -r '.data.result[0].value[1] // "0" | tonumber | floor'; }
lag_before=$(lag "$scaled_at"); lag_after=$(lag "$verified_at")
((lag_after < lag_before)) || fail "lag didn't fall: $lag_before at the rescale, $lag_after at verification"
pass "parallelism → $to_parallelism (tenant maximum $max); lag $lag_before → $lag_after records by verification"

step "F4: escalated to the owning team with evidence"
f4=$(incident positions-aggregator)
[[ -n "$f4" ]] || fail "no incident for positions-aggregator"
[[ $(jq -r .stage <<<"$f4") == escalated ]] || fail "positions-aggregator: $(jq -c '{stage, remediation, outcome}' <<<"$f4")"
id=$(jq -r .incident_id <<<"$f4")
ticket=$(tickets "correlation_id=$id" | jq -c '.result[0] // empty')
[[ $(jq -r .assignment_group <<<"$ticket") == risk-platform ]] ||
  fail "ticket $(jq -r .number <<<"$ticket") is assigned to '$(jq -r .assignment_group <<<"$ticket")', not risk-platform"
grep -q 'State keeps growing' <<<"$(jq -r .work_notes <<<"$ticket")" || fail "the escalation note lacks the evidence"
pass "$(jq -r .number <<<"$ticket") assigned to risk-platform: $(jq -r .outcome <<<"$f4")"

token=$(curl -sf -i "$MM/users/login" -H 'Content-Type: application/json' \
  -d "{\"login_id\": \"$ADMIN_USER\", \"password\": \"$ADMIN_PASSWORD\"}" |
  tr -d '\r' | awk -F': ' 'tolower($1) == "token" {print $2}')
[[ -n "$token" ]] || fail "couldn't log in to Mattermost as $ADMIN_USER"
mm() { curl -sf -H "Authorization: Bearer $token" "$@"; }
team=$(mm "$MM/teams/name/platform" | jq -r .id)
channel=$(mm "$MM/teams/$team/channels/name/platform-incidents" | jq -r .id)
# Every post in the channel, thread replies included.
posts='{"posts": {}}'
for page in $(seq 0 20); do
  batch=$(mm "$MM/channels/$channel/posts?page=$page&per_page=200")
  posts=$(jq -s '{posts: (.[0].posts + .[1].posts)}' <(echo "$posts") <(echo "$batch"))
  (($(jq '.order | length' <<<"$batch") == 200)) || break
done
posts=$(jq '{posts: [.posts[]]}' <<<"$posts")
card=$(jq -c '[.posts[] | select(.props.attachments[0].actions // [] | length > 0)
  | select(.props.attachments[0].title | startswith("positions-aggregator"))] | first // empty' <<<"$posts")
[[ -n "$card" ]] || fail "no escalation card with buttons for positions-aggregator in #platform-incidents"
labels=$(jq -r '[.props.attachments[0].actions[].name] | join(" / ")' <<<"$card")
[[ $labels == "Acknowledge / Restart with state on disk (temporary)" ]] || fail "the card offers: $labels"
pass "escalation card offers: $labels"

post_id=$(jq -r .id <<<"$card")
mm -X POST "$MM/posts/$post_id/actions/button1" -H 'Content-Type: application/json' -d '{}' >/dev/null ||
  fail "Mattermost refused the button click"
for _ in $(seq 60); do
  restarted=$(sql "SELECT count(*) FROM deployments WHERE job_name = 'positions-aggregator' AND ts >= '$started'
                   AND outcome = 'succeeded' AND '--state-backend=rocksdb' = ANY(args)")
  ((restarted > 0)) && break
  sleep 5
done
((restarted > 0)) || fail "clicking the temporary restart didn't restart positions-aggregator with its state on disk"
state=$(curl -sf localhost:8081/jobs/overview | jq -r '[.jobs[] | select(.name == "positions-aggregator" and .state != "CANCELED" and .state != "FINISHED")][0].state')
[[ $state == RUNNING ]] || fail "positions-aggregator is $state after the restart"
pass "approved through Mattermost: restarted with its state on disk, and RUNNING"

step "Guardrails and the audit trail"
automatic=$(sql "SELECT string_agg(DISTINCT job_name, ', ') FROM actions
                 WHERE ts >= '$started' AND actor = 'operator:guardrails' AND phase = 'approved'")
[[ $automatic == "market-data-to-iceberg, payments-enrichment" ]] ||
  fail "automatic actions on: ${automatic:-none} (expected only payments-enrichment and market-data-to-iceberg)"
pass "automatic actions only on F1 and F3; every other incident escalated or followed up"
missing=$(jq -r '[.[] | select(.ticket == null) | .job_name] | join(", ")' <<<"$incidents")
[[ -z "$missing" ]] || fail "incidents without a ticket: $missing"
unaudited=$(for id in $(jq -r '.[].incident_id' <<<"$incidents"); do
  [[ $(sql "SELECT count(*) FROM actions WHERE incident_id = '$id' AND phase = 'proposed'") -gt 0 ]] || echo "$id"
done)
[[ -z "$unaudited" ]] || fail "incidents with no proposal in platform.actions: $unaudited"
cards=$(jq '[.posts[] | select(.root_id == "" and (.props.attachments[0].footer // "" | startswith("operator-bot")))] | length' <<<"$posts")
((cards >= $(jq length <<<"$incidents"))) || fail "$cards incident cards for $(jq length <<<"$incidents") incidents"
pass "every incident has a ticket, a card in #platform-incidents and a proposal in platform.actions"

printf '\n\033[32mOperator test passed.\033[0m\n'
