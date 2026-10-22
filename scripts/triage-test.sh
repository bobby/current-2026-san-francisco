#!/usr/bin/env bash
# The triage agent's done-when check (issue #9): on the scripted timeline,
# F1–F4 and the fleet's hidden problems are raised, and the noise (§7.2) is
# suppressed or grouped. Needs the stack up and JEV_API_KEY in .env.
#
#   scripts/triage-test.sh            reset, run the whole timeline (~35 min), check
#   scripts/triage-test.sh --no-run   check the latest run again
#
# Checks:
#  1. Each starring job (F1–F4) and each of the fleet's genuine problems has
#     a raised signal, after its fault began.
#  2. Few other jobs were raised: at most one per NOISE_PER alerts (at least
#     2), so the budget scales with the fleet (200 jobs locally, 1,000 on
#     AWS). The rest of the alerts were suppressed or attached.
#  3. Every raised signal has a card in #platform-signals, and every signal
#     records the state, questions and answers Jev saw.
set -euo pipefail

cd "$(dirname "$0")/.."

NOISE_PER=${NOISE_PER:-250}
MM=http://localhost:8065/api/v4
ADMIN_PASSWORD=${MATTERMOST_ADMIN_PASSWORD:-oncall-demo-2026}

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }
step() { printf '\n%s\n' "$*"; }
sql() { docker compose exec -T postgres psql -U ops -d ops -Atc "$1"; }

[[ $(curl -sf localhost:8093/stats | jq -r .enabled) == true ]] ||
  fail "the triage agent isn't triaging; set JEV_API_KEY in .env and run 'make up'"

if [[ ${1:-} != --no-run ]]; then
  scripts/director.sh reset
  scripts/director.sh run --scenario all
fi

started=$(jq -r '.run.started_at // empty' .director/state.json)
[[ -n "$started" ]] || fail "no director run to check"
# Judgments of the run's last alerts land a few minutes after it ends.
ends=$(jq -r .run.ends_at .director/state.json)
# The run's verdicts: its alerts', judged up to 5 minutes after it ends.
until=$(sql "SELECT to_char('$ends'::timestamptz + interval '5 minutes', 'YYYY-MM-DD\"T\"HH24:MI:SSOF')")
sql "SELECT 1" >/dev/null
for _ in $(seq 60); do
  [[ $(sql "SELECT count(*) FROM alerts WHERE status = 'firing' AND ts >= '$started' AND ts < '$ends'
              AND event_id::text NOT IN (SELECT jsonb_array_elements_text(event->'alert_event_ids') FROM signals)") == 0 ]] && break
  sleep 5
done

roster=$(docker compose config --format json | jq -r '.services["st-fleet"].volumes[] | select(.target == "/fleet/roster.json") | .source')
genuine=$(jq -r '.[] | select(.profile | IN("crash_loop", "stalled", "state_growth")) | .job_name' "$roster")

step "Raised: the timeline's faults and the fleet's genuine problems"
raised_at() {
  sql "SELECT to_char(min(ts) - '$started'::timestamptz, 'MI:SS') FROM signals
       WHERE job_name = '$1' AND disposition = 'raise' AND ts >= '$started'::timestamptz + '$2'::interval
         AND ts < '$until'"
}
expect() {  # <job> <fault starts at> <what>
  local at; at=$(raised_at "$1" "$2")
  [[ -n "$at" ]] || fail "$3: $1 wasn't raised"
  pass "$3: $1 raised at $at"
}
expect trade-events-ingest 11min "F2 (poison records)"
expect payments-enrichment 15min "F1 (bad deploy)"
expect market-data-to-iceberg 18min "F3 (traffic spike)"
expect positions-aggregator 1min "F4 (state growth)"
for job in $genuine; do
  expect "$job" 0min "fleet ($(jq -r --arg j "$job" '.[] | select(.job_name == $j) | .profile' "$roster"))"
done

step "Noise: suppressed or grouped"
expected=$(printf "'%s'," trade-events-ingest payments-enrichment market-data-to-iceberg positions-aggregator $genuine)
read -r alerts judgments raised attached suppressed alerts_suppressed < <(sql "
  SELECT (SELECT count(*) FROM alerts WHERE status = 'firing' AND ts >= '$started' AND ts < '$ends'),
         count(*), count(*) FILTER (WHERE disposition = 'raise'), count(*) FILTER (WHERE disposition = 'attach'),
         count(*) FILTER (WHERE disposition = 'suppress'),
         (SELECT count(DISTINCT a) FROM signals s2, jsonb_array_elements_text(s2.event->'alert_event_ids') a
           WHERE s2.disposition = 'suppress' AND s2.ts >= '$started' AND s2.ts < '$until')
  FROM signals WHERE ts >= '$started' AND ts < '$until'" | tr '|' ' ')
pass "$alerts alerts in → $judgments judgments (with rechecks): $raised raised, $attached attached, $suppressed suppressed, covering $alerts_suppressed alerts"
noise=$(sql "SELECT job_name || ' (' || string_agg(DISTINCT coalesce(failure_class, '?'), ', ') || ')' FROM signals
             WHERE ts >= '$started' AND ts < '$until' AND disposition = 'raise' AND job_name NOT IN (${expected%,})
             GROUP BY job_name ORDER BY min(ts)")
count=$(grep -c . <<<"$noise" || true)
budget=$((alerts / NOISE_PER > 2 ? alerts / NOISE_PER : 2))
((count <= budget)) || fail "$count other jobs raised (at most $budget for $alerts alerts): $(paste -sd ';' <<<"$noise")"
pass "$count other jobs raised (at most $budget for $alerts alerts)${noise:+: $(paste -sd ';' - <<<"$noise" | sed 's/;/; /g')}"

step "Cards and audit trail"
token=$(curl -sf -i "$MM/users/login" -H 'Content-Type: application/json' \
  -d "{\"login_id\": \"oncall\", \"password\": \"$ADMIN_PASSWORD\"}" |
  tr -d '\r' | awk -F': ' 'tolower($1) == "token" {print $2}')
[[ -n "$token" ]] || fail "couldn't log in to Mattermost as oncall"
team=$(curl -sf -H "Authorization: Bearer $token" "$MM/teams/name/platform" | jq -r .id)
channel=$(curl -sf -H "Authorization: Bearer $token" "$MM/teams/$team/channels/name/platform-signals" | jq -r .id)
since=$(jq -rn --arg t "$started" '$t | sub("\\.[0-9]+"; "") | fromdateiso8601 * 1000')
before=$(jq -rn --arg t "$until" '$t | sub("\\+00$"; "Z") | fromdateiso8601 * 1000')
cards=$(curl -sf -H "Authorization: Bearer $token" "$MM/channels/$channel/posts?since=$since&per_page=200" |
  jq --argjson before "$before" '[.posts[] | select(.root_id == "" and .create_at < $before and (.props.attachments // [] | length) > 0)] | length')
((cards == raised)) || fail "$cards cards in #platform-signals for $raised raised signals"
pass "$cards cards in #platform-signals, one per raised signal"
missing=$(sql "SELECT count(*) FROM signals WHERE ts >= '$started' AND ts < '$until'
               AND NOT (event->'judgment' ?& array['state', 'questions', 'answers', 'summary'])")
((missing == 0)) || fail "$missing signals lack their state, questions or answers"
pass "every signal records Jev's state, questions and answers"

printf '\n\033[32mTriage test passed.\033[0m\n'
