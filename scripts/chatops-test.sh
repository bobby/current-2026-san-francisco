#!/usr/bin/env bash
# End-to-end test for the human interfaces (issue #8). Run it with the stack
# up (`make chatops-test`).
#
#  1. The chatops-bot has set up Mattermost: the team, the three channels and
#     the bot accounts.
#  2. Alerts flow from Alertmanager's webhook into #platform-alerts.
#  3. The tickets mock speaks the ServiceNow Table API: create, read, query,
#     update with work notes; bad queries are refused.
#  4. A ticket created through the API shows as a card in
#     #platform-incidents that links to its UI page (with Open Graph tags),
#     and the card follows updates.
#  5. An approval card's button, clicked through Mattermost, calls back to
#     the bot, which records the decision in platform.actions and replaces
#     the buttons.
#  6. Mentioning a ticket number in a channel gets the ticket's card in reply
#     (the bot follows Mattermost's WebSocket).
#  7. Grafana serves the platform dashboard, and both of its datasources
#     answer queries.
set -euo pipefail

cd "$(dirname "$0")/.."

MM=http://localhost:8065/api/v4
BOT=http://localhost:8092
TICKETS=http://localhost:8094
GRAFANA=http://localhost:3000
COLLECTOR=http://localhost:8090
ADMIN_USER=oncall
ADMIN_PASSWORD=${MATTERMOST_ADMIN_PASSWORD:-oncall-demo-2026}
GRAFANA_AUTH=admin:${GRAFANA_ADMIN_PASSWORD:-admin}
RUN=chatops-test-$(date +%s)

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

curl -sf "$BOT/health" >/dev/null || fail "the chatops-bot isn't ready; run 'make up'"

token=$(curl -sf -i "$MM/users/login" -H 'Content-Type: application/json' \
  -d "{\"login_id\": \"$ADMIN_USER\", \"password\": \"$ADMIN_PASSWORD\"}" |
  tr -d '\r' | awk -F': ' 'tolower($1) == "token" {print $2}')
[[ -n "$token" ]] || fail "couldn't log in to Mattermost as $ADMIN_USER"
mm() { curl -sf -H "Authorization: Bearer $token" "$@"; }

step "Mattermost workspace"
team=$(mm "$MM/teams/name/platform" | jq -r .id)
declare -A channel
for name in platform-alerts platform-signals platform-incidents; do
  channel[$name]=$(mm "$MM/teams/$team/channels/name/$name" | jq -r .id)
  [[ -n "${channel[$name]}" && "${channel[$name]}" != null ]] || fail "channel $name is missing"
done
for bot in alertmanager tickets triage-bot operator-bot ops-assistant; do
  [[ "$(mm "$MM/users/username/$bot" | jq -r .is_bot)" == true ]] || fail "bot $bot is missing"
done
pass "team platform, channels #platform-alerts, #platform-signals, #platform-incidents, and 5 bots"

# posts_in <channel> <jq filter>: the channel's recent posts matching the filter.
posts_in() {
  mm "$MM/channels/${channel[$1]}/posts?per_page=100" | jq -c "[.order[] as \$id | .posts[\$id]] | map(select($2))"
}
has_post() { [[ "$(posts_in "$1" "$2" | jq length)" -gt 0 ]]; }

step "Alert firehose"
starts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -n --arg starts "$starts" --arg run "$RUN" '{
  version: "4", status: "firing", receiver: "collector", alerts: [{
    status: "firing", fingerprint: $run,
    labels: {alertname: "ChatopsTestAlert", severity: "warning", job_name: "payments_enrichment"},
    annotations: {summary: $run},
    startsAt: $starts, endsAt: "0001-01-01T00:00:00Z", generatorURL: "http://prometheus:9090/graph"}]}' |
  curl -sf -X POST "$COLLECTOR/alertmanager" -H 'Content-Type: application/json' -d @- >/dev/null
retry 60 has_post platform-alerts ".props.attachments[0].text == \"$RUN\"" ||
  fail "the alert didn't reach #platform-alerts"
card=$(posts_in platform-alerts ".props.attachments[0].text == \"$RUN\"" | jq '.[0].props.attachments[0]')
[[ "$(jq -r .title <<<"$card")" == "FIRING: ChatopsTestAlert" ]] || fail "unexpected alert card: $card"
pass "alert → collector → platform.alerts → #platform-alerts, as $(jq -r '.fields | map(.title + "=" + .value) | join(", ")' <<<"$card")"

step "Tickets: the ServiceNow Table API"
api() { curl -sf -u "$RUN:" -H 'Content-Type: application/json' -H 'Accept: application/json' "$@"; }
created=$(api -X POST "$TICKETS/api/now/table/incident" -d "$(jq -n --arg run "$RUN" '{
  short_description: "payments-enrichment restart loop (\($run))", description: "NullPointerException after 1.42.0",
  priority: 2, cmdb_ci: "payments-enrichment", assignment_group: "payments-eng", work_notes: "Opened by \($run)"}')")
sys_id=$(jq -r .result.sys_id <<<"$created")
number=$(jq -r .result.number <<<"$created")
[[ "$number" =~ ^INC[0-9]{7}$ ]] || fail "unexpected number $number"
[[ "$(jq -r '.result | .state + " " + .priority + " " + .sys_created_by' <<<"$created")" == "1 2 $RUN" ]] ||
  fail "unexpected record: $created"
[[ "$(api "$TICKETS/api/now/table/incident/$sys_id" | jq -r .result.number)" == "$number" ]] || fail "GET by sys_id failed"
found=$(api -G "$TICKETS/api/now/table/incident" --data-urlencode "sysparm_query=cmdb_ci=payments-enrichment^active=true^short_descriptionLIKE$RUN" \
  --data-urlencode "sysparm_fields=number,state")
[[ "$(jq -c .result <<<"$found")" == "[{\"number\":\"$number\",\"state\":\"1\"}]" ]] || fail "query returned $found"
status=$(curl -s -o /dev/null -w '%{http_code}' -G "$TICKETS/api/now/table/incident" --data-urlencode 'sysparm_query=nonsense=1')
[[ "$status" == 400 ]] || fail "a bad sysparm_query answered $status, not 400"
pass "$number created, read by sys_id, found by encoded query; bad queries are refused (400)"

step "Ticket card in #platform-incidents"
is_card="(.props.attachments[0].title // \"\") | startswith(\"$number:\")"
retry 30 has_post platform-incidents "$is_card" || fail "no card for $number in #platform-incidents"
link=$(posts_in platform-incidents "$is_card" | jq -r '.[0].props.attachments[0].title_link')
[[ "$link" == "$TICKETS/incident/$number" ]] || fail "the card links to $link"
page=$(curl -sf "$link")
grep -q "<meta property=\"og:title\" content=\"$number: payments-enrichment restart loop" <<<"$page" ||
  fail "the ticket page has no Open Graph title"
pass "card links to $link, which has Open Graph tags"

updated=$(api -X PATCH "$TICKETS/api/now/table/incident/$sys_id" \
  -d '{"state": "2", "assigned_to": "operator-bot", "work_notes": "Rolled back to 1.41.0"}')
[[ "$(jq -r .result.work_notes <<<"$updated")" == *"Rolled back to 1.41.0"*"Opened by"* ]] ||
  fail "work notes aren't newest first: $(jq .result.work_notes <<<"$updated")"
card_state() {
  posts_in platform-incidents "$is_card" |
    jq -e '.[0].props.attachments[0].fields | any(.title == "State" and .value == "In Progress")'
}
retry 30 card_state || fail "the card didn't follow the update"
retry 30 has_post platform-incidents ".message | contains(\"$number\") and contains(\"Rolled back to 1.41.0\")" ||
  fail "the work note wasn't posted to the thread"
pass "PATCH (state, assignee, work note): the card shows In Progress and the note is in its thread"

step "Approval buttons"
action_id=$(uuidgen | tr 'A-Z' 'a-z')
posted=$(jq -n --arg id "$action_id" '{channel: "platform-incidents", card: {
  card: "approval", action_id: $id, job_name: "payments-enrichment", incident_id: null, signal_id: null,
  title: "Keep watching payments-enrichment", rationale: "chatops-test approval", proposed_by: "chatops-test",
  fields: [], buttons: [
    {label: "Approve", style: "primary", action: "observe", params: {minutes: 10}, phase: "approved"},
    {label: "Reject", style: "danger", action: "observe", params: {minutes: 10}, phase: "rejected"}]}}' |
  curl -sf -X POST "$BOT/api/posts" -H 'Content-Type: application/json' -d @-)
post_id=$(jq -r .post_id <<<"$posted")
[[ -n "$post_id" && "$post_id" != null ]] || fail "couldn't post an approval card: $posted"
# Click Approve as the on-call engineer, through Mattermost.
mm -X POST "$MM/posts/$post_id/actions/button0" -H 'Content-Type: application/json' -d '{}' >/dev/null ||
  fail "Mattermost refused the button click (is the chatops-bot an allowed internal connection?)"
recorded() {
  [[ "$(sql "select actor || ' ' || action || ' ' || phase from actions where action_id = '$action_id'")" == "user:$ADMIN_USER observe approved" ]]
}
retry 30 recorded || fail "the decision isn't in platform.actions / the ops DB"
after=$(mm "$MM/posts/$post_id" | jq '.props.attachments[0]')
[[ "$(jq 'has("actions")' <<<"$after")" == false && "$(jq -r .pretext <<<"$after")" == *"Approved by @$ADMIN_USER"* ]] ||
  fail "the card still offers the buttons: $after"
pass "Approve → bot callback → platform.actions (user:$ADMIN_USER, approved); the buttons became the decision"

step "Ticket lookup"
mm -X POST "$MM/posts" -H 'Content-Type: application/json' \
  -d "$(jq -n --arg c "${channel[platform-incidents]}" --arg m "what's the status of $number?" '{channel_id: $c, message: $m}')" >/dev/null
lookup="(.root_id != \"\") and ((.props.attachments[0].title // \"\") | startswith(\"$number:\"))"
retry 30 has_post platform-incidents "$lookup" || fail "no reply with the ticket card"
pass "a post mentioning $number got its card in the thread"

step "Grafana"
curl -sf "$GRAFANA/api/health" >/dev/null || fail "Grafana isn't healthy"
title=$(curl -sf "$GRAFANA/api/dashboards/uid/platform-overview" | jq -r .dashboard.title)
[[ "$title" == "Platform overview" ]] || fail "the dashboard isn't provisioned (anonymous view)"
ds_query() {
  curl -sf -u "$GRAFANA_AUTH" -X POST "$GRAFANA/api/ds/query" -H 'Content-Type: application/json' -d "$1"
}
alerts=$(ds_query '{"from": "now-1h", "to": "now", "queries": [{"refId": "A", "datasource": {"uid": "ops-db"},
  "rawSql": "SELECT count(*) AS n FROM alerts", "format": "table"}]}' | jq '.results.A.frames[0].data.values[0][0]')
[[ "$alerts" =~ ^[0-9]+$ && "$alerts" -gt 0 ]] || fail "the ops DB datasource returned $alerts"
series=$(ds_query '{"from": "now-5m", "to": "now", "queries": [{"refId": "A", "datasource": {"uid": "prometheus"},
  "expr": "count(flink_jobmanager_job_numRestarts)", "instant": true}]}' | jq '.results.A.frames[0].data.values[1][0]')
[[ "$series" =~ ^[0-9]+$ && "$series" -gt 0 ]] || fail "the Prometheus datasource returned $series"
pass "dashboard provisioned and viewable without login; ops DB ($alerts alerts) and Prometheus ($series jobs) answer"

printf '\n\033[32mChatops test passed.\033[0m\n'
