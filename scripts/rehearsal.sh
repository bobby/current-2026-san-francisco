#!/usr/bin/env bash
# A full dress rehearsal (issue #12; docs/runbook.md): the whole timeline as
# on stage, with the engineer's part in Scenario 2 played by script, then
# every scenario's checks, the timings of what the audience sees, the
# containers' peak memory, and a recording of the run for `director replay`.
# Needs the stack up, and JEV_API_KEY and ANTHROPIC_API_KEY in .env. About
# 45 minutes; run it with sleep prevented on a laptop (caffeinate -is).
#
#   scripts/rehearsal.sh               rehearse, check, time and record
#   scripts/rehearsal.sh --no-record   the same, without saving a recording
#
# A rehearsal passes when nobody had to touch anything: preflight is clean,
# the director's steps all ran, and the assistant, triage and operator checks
# (scripts/{assistant,triage,operator}-test.sh) pass on this one run. The
# logs, timings.txt and memory.txt go to .director/rehearsals/<start>/; the
# recording to recordings/.
set -uo pipefail

cd "$(dirname "$0")/.."

RECORD=true
[[ ${1:-} == --no-record ]] && RECORD=false
out=.director/rehearsals/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$out"

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
miss() { printf '  \033[31m✗\033[0m %s\n' "$*"; failures+=("$*"); }
step() { printf '\n\033[1m%s\033[0m\n' "$*"; }
sql() { docker compose exec -T postgres psql -U ops -d ops -Atc "$1"; }
failures=()

# check <name> <command...>: runs one check, logging to $out/<name>.log and
# showing its results indented.
check() {
  local name=$1
  shift
  "$@" >"$out/$name.log" 2>&1
  local status=$?
  grep -E '✓|✗|│' "$out/$name.log" | sed 's/^/  /'
  if ((status == 0)); then pass "$name check passed"; else miss "$name check failed (see $out/$name.log)"; fi
}

# Every 30 s, each platform container's memory in MiB: epoch, name, MiB.
sample_memory() {
  while :; do
    docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}' 2>/dev/null | awk -F'\t' -v t="$(date +%s)" '
      $1 ~ /^platform-/ {
        split($2, a, " / "); v = a[1]; n = v + 0
        if (v ~ /GiB/) n *= 1024; else if (v ~ /KiB/) n /= 1024; else if (v !~ /MiB/) n /= 1048576
        printf "%s\t%s\t%.0f\n", t, $1, n
      }' >>"$out/memory.tsv"
    sleep 30
  done
}

step "Preflight"
check preflight scripts/preflight.sh
((${#failures[@]} == 0)) || { printf '\nPreflight failed; not rehearsing.\n'; exit 1; }

step "Reset"
scripts/director.sh reset | tee "$out/reset.log" | sed 's/^/  /'
((PIPESTATUS[0] == 0)) || { miss "the reset failed"; exit 1; }

step "Running the whole timeline (30 minutes), with Scenario 2's engineer played by script"
scripts/director.sh run --scenario all >"$out/director.log" 2>&1 &
director=$!
sample_memory &
sampler=$!
trap 'kill $director $sampler 2>/dev/null || true' EXIT
started=""
for _ in $(seq 60); do
  started=$(jq -r '.run.started_at // empty' .director/state.json 2>/dev/null)
  [[ -n "$started" ]] && break
  sleep 2
done
[[ -n "$started" ]] || { miss "the run didn't start"; exit 1; }
pass "run started at $started"

check assistant scripts/assistant-test.sh --in-run

wait $director
director_status=$?
sed 's/^/  /' "$out/director.log" | grep -E '^\s+\[' || true
((director_status == 0)) && pass "every timeline step ran" || miss "the director reported failed steps"

step "Scenario checks on this run"
check triage scripts/triage-test.sh --no-run
check operator scripts/operator-test.sh --no-run
kill $sampler 2>/dev/null

step "Timings (from the run's start)"
roster=$(docker compose config --format json | jq -r '.services["st-fleet"].volumes[] | select(.target == "/fleet/roster.json") | .source')
jobs=$(printf "'%s'," trade-events-ingest payments-enrichment market-data-to-iceberg positions-aggregator \
  $(jq -r '.[] | select(.profile | IN("crash_loop", "stalled", "state_growth")) | .job_name' "$roster"))
{
  echo "Timeline steps"
  jq -r '.run.steps[] | "  \(.at_secs / 60 | floor | tostring | if length < 2 then "0" + . else . end):\(.at_secs % 60 | tostring | if length < 2 then "0" + . else . end)  \(.action) — \(.note)"' .director/state.json
  echo
  echo "First symptoms"
  scripts/director.sh incidents --times | sed 's/^/  /'
  echo
  echo "What the audience sees (signals, actions, deploys)"
  sql "SELECT '  ' || to_char(ts - '$started'::timestamptz, 'MI:SS') || '  ' || rpad(job_name, 34) || what FROM (
         SELECT ts, job_name, 'signal raised (' || coalesce(failure_class, '?') || ')' AS what
           FROM signals WHERE disposition = 'raise' AND ts >= '$started'
         UNION ALL
         SELECT ts, job_name, actor || ' ' || phase || ' ' || action FROM actions WHERE ts >= '$started'
         UNION ALL
         SELECT ts, job_name, kind || ' ' || to_version || ' by ' || triggered_by || ' (' || outcome || ')'
           FROM deployments WHERE ts >= '$started' AND triggered_by <> 'director'
       ) e WHERE job_name IN (${jobs%,}) ORDER BY ts"
  echo
  echo "Scenario 2's conversation"
  grep -E 'answered in|redeployed|opened for|RUNNING with' "$out/assistant.log" | sed 's/^ */  /'
} >"$out/timings.txt"
cat "$out/timings.txt"

step "Memory (peak per container, sampled every 30 s)"
if [[ -s $out/memory.tsv ]]; then
  {
    awk -F'\t' '{ n = $2; sub(/^platform-/, "", n); if ($3 > peak[n]) peak[n] = $3; total[$1] += $3 }
      END {
        for (n in peak) printf "  %-26s %6d MiB\n", n, peak[n]
        for (t in total) if (total[t] > max) max = total[t]
        printf "  %-26s %6d MiB\n", "TOTAL (peak of the sum)", max
      }' "$out/memory.tsv" | sort -k2 -n -r
    if command -v free >/dev/null; then
      echo "  host: $(free -m | awk '/^Mem:/ {printf "%d MiB total, %d MiB available now", $2, $7}')"
    else
      echo "  Docker VM: $(docker info --format '{{.MemTotal}}' | awk '{printf "%d MiB", $1 / 1048576}')"
    fi
  } >"$out/memory.txt"
  cat "$out/memory.txt"
fi

if $RECORD; then
  step "Recording the run for the replay fallback"
  scripts/director.sh record | tee "$out/record.log" | sed 's/^/  /'
  ((PIPESTATUS[0] == 0)) || miss "recording failed"
fi

if ((${#failures[@]} == 0)); then
  printf '\n\033[32mRehearsal passed without intervention.\033[0m Logs in %s\n' "$out"
else
  printf '\n\033[31mRehearsal failed:\033[0m\n' >&2
  printf '  - %s\n' "${failures[@]}" >&2
  printf 'Logs in %s\n' "$out" >&2
  exit 1
fi
