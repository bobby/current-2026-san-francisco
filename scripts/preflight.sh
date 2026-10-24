#!/usr/bin/env bash
# Go/no-go before a rehearsal or the talk (issue #12; docs/runbook.md): is the
# stack healthy, reset and ready to run, and can the agents reach their
# models? It takes about 30 seconds and changes nothing. Run it where the
# stack runs: locally, or on the instance (`scripts/aws.sh exec
# scripts/preflight.sh`).
#
#   scripts/preflight.sh            check the stack
#   scripts/preflight.sh --tunnel   from the laptop: check the UIs forwarded from EC2
#
# Each check prints ✓ (fine), ! (look at it) or ✗ (fix it before going on);
# it exits non-zero if any ✗.
set -uo pipefail

cd "$(dirname "$0")/.."

MM=http://localhost:8065/api/v4
ADMIN_PASSWORD=${MATTERMOST_ADMIN_PASSWORD:-oncall-demo-2026}
fails=0

ok() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
bad() { printf '  \033[31m✗\033[0m %s\n' "$*"; fails=$((fails + 1)); }
http() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@"; }

if [[ ${1:-} == --tunnel ]]; then
  echo "UIs forwarded from EC2 (make tunnel)"
  for target in "8065 /api/v4/system/ping Mattermost" "3000 /api/health Grafana" "8094 / the tickets UI" \
    "8081 /overview the Flink UI" "9090 /-/ready Prometheus"; do
    read -r port path name <<<"$target"
    code=$(http "http://localhost:$port$path")
    [[ $code == 200 ]] && ok "$name on :$port" || bad "$name on :$port answered ${code:-nothing}; is 'make tunnel' running?"
  done
  ((fails == 0)) || exit 1
  exit 0
fi

echo "Containers"
problems=$(docker compose ps -a --format json | jq -rs '
  (if type == "array" and (.[0] | type) == "array" then .[0] else . end)[]
  | select((.State == "running" and (.Health == "" or .Health == "healthy")) or (.State == "exited" and .ExitCode == 0) | not)
  | "\(.Service) \(.State) \(.Health)"')
expected=$(docker compose config --services | wc -l | tr -d ' ')
running=$(docker compose ps --status running --format '{{.Service}}' | sort -u | wc -l | tr -d ' ')
[[ -z "$problems" ]] && ok "$running services running and healthy (of $expected, counting one-shot setup)" ||
  bad "unhealthy: $(paste -sd ',' <<<"$problems" | sed 's/,/, /g')"

echo "Flink"
overview=$(curl -sf --max-time 10 localhost:8081/overview)
if [[ -n "$overview" ]]; then
  read -r tms total free < <(jq -r '"\(.taskmanagers) \(."slots-total") \(."slots-available")"' <<<"$overview")
  ((tms > 0)) && ok "$tms TaskManagers, $free of $total slots free" || bad "no TaskManagers registered"
  # F3's rescale doubles market-data-to-iceberg's parallelism.
  ((free >= 4)) || warn "only $free slots free; F3's rescale needs a few"
else
  bad "the Flink REST API isn't answering on :8081"
fi

echo "Director"
run=$(jq -c '.run // empty' .director/state.json 2>/dev/null)
reset_at=$(jq -r '.reset_at // empty' .director/state.json 2>/dev/null)
if [[ -n "$reset_at" ]]; then
  ok "reset at $reset_at, no run since: ready for 'make run'"
  catalogue=$(curl -sf --max-time 10 localhost:8091/catalogue | jq -r '.jobs | keys[]')
  flink=$(curl -sf --max-time 10 localhost:8081/jobs/overview | jq -r '.jobs[] | select(.state == "RUNNING") | .name')
  missing=$(comm -23 <(sort <<<"$catalogue") <(sort -u <<<"$flink"))
  [[ -z "$missing" ]] && ok "every catalogue job RUNNING ($(grep -c . <<<"$catalogue"))" || bad "not RUNNING: $(paste -sd ' ' <<<"$missing")"
elif [[ -n "$run" ]]; then
  if [[ $(jq -r .outcome <<<"$run") == running ]]; then
    warn "a run is in progress: $(scripts/director.sh status 2>/dev/null | head -1)"
  else
    warn "the last run has finished; 'make reset' before the next one"
  fi
else
  warn "no reset yet; 'make reset' before the run"
fi
if docker compose --profile replay ps --status running --format '{{.Service}}' | grep -qx replay; then
  warn "a replay is playing; 'make reset' goes live again"
fi

echo "Agents and models"
[[ $(curl -sf --max-time 5 localhost:8093/stats | jq -r .enabled) == true ]] && ok "triage is triaging" ||
  bad "triage isn't triaging (JEV_API_KEY in .env, then 'make up')"
[[ $(curl -sf --max-time 5 localhost:8095/stats | jq -r .enabled) == true ]] && ok "the operator is operating" ||
  bad "the operator isn't operating (JEV_API_KEY in .env, then 'make up')"
# Reachability and latency only: Jev's API isn't called, so nothing is spent.
jev=$(docker compose exec -T triage sh -c 'curl -s -o /dev/null -w "%{http_code} %{time_total}" --max-time 10 "$JEV_BASE_URL"' 2>/dev/null)
[[ -n "$jev" && ${jev%% *} != 000 ]] && ok "Jev's API reachable from the agents (${jev#* }s)" ||
  bad "Jev's API unreachable from the agents: is the network up? (fallback: the replay)"
# Listing models checks Claude's key without spending tokens.
claude=$(docker compose exec -T chatops-bot sh -c '[ -n "$ANTHROPIC_API_KEY" ] || { echo nokey; exit; }
  curl -s -o /dev/null -w "%{http_code} %{time_total}" --max-time 10 https://api.anthropic.com/v1/models \
    -H "x-api-key: $ANTHROPIC_API_KEY" -H "anthropic-version: 2023-06-01"' 2>/dev/null)
case "${claude%% *}" in
  200) ok "Claude's key works (${claude#* }s)" ;;
  nokey) bad "the chatops-bot has no ANTHROPIC_API_KEY (.env, then 'make up')" ;;
  401 | 403) bad "Anthropic rejected the key (HTTP ${claude%% *})" ;;
  *) bad "Anthropic's API unreachable from the chatops-bot (${claude:-no answer}): is the network up? (fallback: the replay)" ;;
esac

echo "Fleet, chat and dashboards"
series=$(curl -sf --max-time 10 localhost:9249/metrics | grep -c '^flink_jobmanager_job_uptime')
((series > 0)) && ok "the fleet-exporter serves $series simulated jobs" || bad "the fleet-exporter serves no jobs yet (st-fleet starts last: wait a minute after a reset or make up)"
token=$(curl -s -i --max-time 10 "$MM/users/login" -H 'Content-Type: application/json' \
  -d "{\"login_id\": \"oncall\", \"password\": \"$ADMIN_PASSWORD\"}" | tr -d '\r' | awk -F': ' 'tolower($1) == "token" {print $2}')
if [[ -n "$token" ]]; then
  team=$(curl -sf -H "Authorization: Bearer $token" "$MM/teams/name/platform" | jq -r .id)
  channels=$(curl -sf -H "Authorization: Bearer $token" "$MM/teams/$team/channels?per_page=200" |
    jq -r '[.[] | select(.name | IN("platform-alerts", "platform-signals", "platform-incidents"))] | length')
  ((channels == 3)) && ok "Mattermost: oncall logs in; the three channels exist" || bad "Mattermost has $channels of the three channels"
else
  bad "can't log in to Mattermost as oncall"
fi
[[ $(http localhost:3000/api/health) == 200 ]] && ok "Grafana is up" || bad "Grafana isn't answering on :3000"

echo "Resources"
used=$(docker compose exec -T postgres df -P / 2>/dev/null | awk 'NR == 2 {sub(/%/, "", $5); print $5}')
if [[ -z "$used" ]]; then
  warn "couldn't read the disk usage"
elif ((used >= 90)); then
  bad "the Docker disk is ${used}% full ('docker system prune', or see the runbook)"
elif ((used >= 80)); then
  warn "the Docker disk is ${used}% full"
else
  ok "the Docker disk is ${used}% full"
fi
inuse=$(docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}' | awk -F'\t' '$1 ~ /^platform-/ {
  split($2, a, " / "); v = a[1]; n = v + 0
  if (v ~ /GiB/) n *= 1024; else if (v ~ /KiB/) n /= 1024; else if (v !~ /MiB/) n /= 1048576
  s += n } END {printf "%d", s}')
if command -v free >/dev/null; then
  available=$(free -m | awk '/^Mem:/ {print $7}')
  ((available >= 2048)) && ok "containers use ${inuse} MiB; ${available} MiB available on the host" ||
    warn "only ${available} MiB available on the host (containers use ${inuse} MiB)"
else
  vm=$(docker info --format '{{.MemTotal}}' | awk '{printf "%d", $1 / 1048576}')
  ((vm - inuse >= 1536)) && ok "containers use ${inuse} MiB of Docker's ${vm} MiB" ||
    warn "containers use ${inuse} MiB of Docker's ${vm} MiB"
fi

echo "Fallback"
latest=$(ls -t recordings/*.json.gz 2>/dev/null | head -1)
[[ -n "$latest" ]] && ok "a recording to replay: $latest" || warn "no recording in recordings/ for the replay fallback"

echo
if ((fails == 0)); then
  printf '\033[32mGo.\033[0m\n'
else
  printf '\033[31mNo go: %d problem(s) above.\033[0m\n' "$fails"
  exit 1
fi
