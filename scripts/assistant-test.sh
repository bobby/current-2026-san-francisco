#!/usr/bin/env bash
# The ops assistant's done-when check (issue #11): on the F2 timeline, an
# engineer asks @ops-assistant in the triage signal's thread; Claude finds the
# poison records (not the red-herring deploy) and proposes the DLQ redeploy;
# approving it through Mattermost recovers the job. Needs the stack up, and
# JEV_API_KEY and ANTHROPIC_API_KEY in .env.
#
#   scripts/assistant-test.sh   reset, run Scenario 2 and converse (~25 min)
#
# Checks:
#  1. Triage raises trade-events-ingest after the poison records arrive.
#  2. Asked in that thread, Claude streams status lines, answers naming the
#     poison records' producer (oms-apac-1) as the cause, and proposes a
#     redeploy with --on-deser-error=dlq on an approval card.
#  3. Approve (clicked through Mattermost) redeploys through cicd-sim, opens
#     a ticket for the upstream team, and the job stays up.
#  4. A follow-up question resumes the thread's session.
#  5. platform.actions records the proposal, the approval and the execution.
set -euo pipefail

cd "$(dirname "$0")/.."

MM=http://localhost:8065/api/v4
TICKETS=http://localhost:8094
ADMIN_USER=oncall
ADMIN_PASSWORD=${MATTERMOST_ADMIN_PASSWORD:-oncall-demo-2026}
JOB=trade-events-ingest

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }
step() { printf '\n%s\n' "$*"; }
sql() { docker compose exec -T postgres psql -U ops -d ops -Atc "$1"; }

docker compose exec -T chatops-bot sh -c 'test -n "$ANTHROPIC_API_KEY"' ||
  fail "the chatops-bot has no ANTHROPIC_API_KEY; set it in .env and run 'make up'"
[[ $(curl -sf localhost:8093/stats | jq -r .enabled) == true ]] ||
  fail "the triage agent isn't triaging; set JEV_API_KEY in .env and run 'make up'"

scripts/director.sh reset
scripts/director.sh run --scenario 2 > .director/assistant-test-run.log 2>&1 &
director=$!
trap 'kill $director 2>/dev/null || true' EXIT
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)

token=$(curl -sf -i "$MM/users/login" -H 'Content-Type: application/json' \
  -d "{\"login_id\": \"$ADMIN_USER\", \"password\": \"$ADMIN_PASSWORD\"}" |
  tr -d '\r' | awk -F': ' 'tolower($1) == "token" {print $2}')
[[ -n "$token" ]] || fail "couldn't log in to Mattermost as $ADMIN_USER"
mm() { curl -sf -H "Authorization: Bearer $token" "$@"; }
team=$(mm "$MM/teams/name/platform" | jq -r .id)
channel=$(mm "$MM/teams/$team/channels/name/platform-signals" | jq -r .id)
assistant=$(mm "$MM/users/username/ops-assistant" | jq -r .id)

step "Triage raises $JOB (waiting up to 20 minutes for the poison records)"
root=""
for _ in $(seq 240); do
  root=$(mm "$MM/channels/$channel/posts?per_page=200" |
    jq -r --arg job "$JOB" '[.posts[] | select(.root_id == "" and .props.job_name == $job and .props.signal_id != null)]
      | sort_by(.create_at) | first | .id // empty')
  [[ -n "$root" ]] && break
  sleep 5
done
[[ -n "$root" ]] || fail "no signal card for $JOB in #platform-signals"
pass "signal card $(mm "$MM/posts/$root" | jq -r '.props.attachments[0].title')"

# Asks in the thread and waits for the assistant's answer (a reply that isn't
# an italic status line). Prints the answer.
ask() {
  local question=$1 since
  since=$(($(date +%s) * 1000))
  mm -X POST "$MM/posts" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg c "$channel" --arg r "$root" --arg m "$question" '{channel_id: $c, root_id: $r, message: $m}')" >/dev/null
  for _ in $(seq 120); do
    answer=$(mm "$MM/posts/$root/thread" | jq -r --arg a "$assistant" --argjson since "$since" \
      '[.posts[] | select(.user_id == $a and .create_at > $since and (.message | length) > 0
        and (.message | startswith("_") | not))] | sort_by(.create_at) | last | .message // empty')
    [[ -n "$answer" ]] && break
    sleep 5
  done
  printf '%s' "$answer"
}

step "Asking @ops-assistant in the signal's thread"
asked=$(($(date +%s) * 1000))
answer=$(ask "@ops-assistant what's going on here? Was it the deploy?")
[[ -n "$answer" ]] || fail "no answer from the assistant within 10 minutes"
[[ $answer != ⚠️* ]] || fail "the assistant failed: $answer"
statuses=$(mm "$MM/posts/$root/thread" | jq -r --arg a "$assistant" --argjson since "$asked" \
  '[.posts[] | select(.user_id == $a and .create_at > $since and (.message | startswith("_")))] | sort_by(.create_at) | .[].message')
(($(grep -c . <<<"$statuses") >= 2)) || fail "fewer than two status lines: $statuses"
pass "$(grep -c . <<<"$statuses") status lines: $(head -3 <<<"$statuses" | paste -sd ' ' -)…"
printf '%s\n' "$answer" | sed 's/^/      │ /'
grep -q 'oms-apac-1' <<<"$answer" || fail "the answer doesn't name the poison records' producer (oms-apac-1)"
pass "the answer names the producer of the malformed records, oms-apac-1"

card=$(mm "$MM/posts/$root/thread" | jq -c --arg a "$assistant" \
  '[.posts[] | select(.user_id == $a and (.props.attachments[0].actions // [] | length) > 0)] | sort_by(.create_at) | last // empty')
[[ -n "$card" ]] || fail "no approval card in the thread"
params=$(jq -r '.props.attachments[0].fields[] | select(.title == "Parameters") | .value' <<<"$card")
grep -q -- '--on-deser-error=dlq' <<<"$params" || fail "the proposal isn't the DLQ redeploy: $params"
pass "approval card: $(jq -r '.props.attachments[0].title' <<<"$card") ($(tr '\n' ' ' <<<"$params"))"

step "Approving it through Mattermost"
approved=$(date -u +%Y-%m-%dT%H:%M:%SZ)
mm -X POST "$MM/posts/$(jq -r .id <<<"$card")/actions/button0" -H 'Content-Type: application/json' -d '{}' >/dev/null ||
  fail "Mattermost refused the click"
for _ in $(seq 60); do
  deployed=$(sql "SELECT to_version FROM deployments WHERE job_name = '$JOB' AND ts >= '$approved' AND outcome = 'succeeded'
                  AND triggered_by = 'user:$ADMIN_USER' AND '--on-deser-error=dlq' = ANY(args) LIMIT 1")
  [[ -n "$deployed" ]] && break
  sleep 3
done
[[ -n "$deployed" ]] || fail "no DLQ redeploy of $JOB after approval"
pass "cicd-sim redeployed $JOB $deployed with --on-deser-error=dlq, triggered by user:$ADMIN_USER"
ticket=""
for _ in $(seq 20); do
  ticket=$(curl -sf -u assistant-test: -H 'Accept: application/json' -G "$TICKETS/api/now/table/incident" \
    --data-urlencode "sysparm_query=cmdb_ci=$JOB^sys_created_by=ops-assistant" | jq -c '.result[0] // empty')
  [[ -n "$ticket" ]] && break
  sleep 3
done
[[ -n "$ticket" ]] || fail "no ticket opened for the approved proposal"
pass "$(jq -r .number <<<"$ticket") opened for $(jq -r .assignment_group <<<"$ticket"): $(jq -r .short_description <<<"$ticket")"

step "The job recovers (watching for 2 minutes)"
for _ in $(seq 24); do
  sleep 5
  failures=$(sql "SELECT count(*) FROM job_status WHERE job_name = '$JOB' AND ts > '$approved'::timestamptz + interval '20 seconds' AND exception_class IS NOT NULL")
  ((failures == 0)) || fail "$JOB failed again after the redeploy"
done
state=$(curl -sf localhost:8081/jobs/overview | jq -r --arg j "$JOB" '[.jobs[] | select(.name == $j and .state != "CANCELED" and .state != "FINISHED")][0].state')
[[ $state == RUNNING ]] || fail "$JOB is $state"
dlq=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic trade-events.dlq 2>/dev/null |
  awk -F: '{s += $3} END {print s + 0}')
((dlq > 0)) || fail "nothing reached trade-events.dlq"
pass "$JOB RUNNING with no failures for 2 minutes; $dlq records diverted to trade-events.dlq"

step "A follow-up resumes the session"
followup=$(ask "@ops-assistant is it healthy now?")
[[ -n "$followup" && $followup != ⚠️* ]] || fail "no answer to the follow-up: $followup"
printf '%s\n' "$followup" | sed 's/^/      │ /'
pass "answered in the same thread's session"

step "Audit trail"
phases=$(sql "SELECT string_agg(actor || ' ' || phase, ', ' ORDER BY ts) FROM actions WHERE action_id = '$(sql "SELECT action_id FROM actions WHERE job_name = '$JOB' AND actor = 'ops-assistant' AND phase = 'proposed' ORDER BY ts DESC LIMIT 1")'")
for want in "ops-assistant proposed" "user:$ADMIN_USER approved" "cicd-sim executed"; do
  grep -q "$want" <<<"$phases" || fail "platform.actions lacks '$want': $phases"
done
pass "platform.actions: $phases"

wait $director 2>/dev/null || true
printf '\n\033[32mAssistant test passed.\033[0m\n'
