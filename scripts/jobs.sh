#!/usr/bin/env bash
# Submit, cancel and list the platform's Flink jobs by hand (issue #2). The
# CI/CD simulator (#6) will take over versioned deploys.
#
#   scripts/jobs.sh submit <job> [job flags...]   e.g. submit payments-enrichment --fault=npe-on-null-field
#   scripts/jobs.sh submit-all                    submit every job, healthy
#   scripts/jobs.sh cancel <job>|all
#   scripts/jobs.sh list
#
# SQL jobs (jobs/sql/<job>.sql) go through the SQL Gateway. Java jobs run from
# jobs/java/target/platform-jobs.jar (build it with `make jobs`), with
# --job=<job> plus any extra flags (--fault, --on-deser-error, ...).
set -euo pipefail

cd "$(dirname "$0")/.."

FLINK=${FLINK:-http://localhost:8081}
GATEWAY=${GATEWAY:-http://localhost:8083}
JAR=jobs/java/target/platform-jobs.jar
JAVA_JOBS=(payments-enrichment trade-events-ingest positions-aggregator)
SQL_JOBS=(market-data-to-iceberg customer-events-to-iceberg audit-log-archiver)

die() { echo "error: $*" >&2; exit 1; }

# Runs one statement in a SQL Gateway session and prints its first result
# field (the job id, for an INSERT). Fails with the gateway's error.
sql_statement() {
  local session=$1 statement=$2 op result
  op=$(jq -n --arg s "$statement" '{statement: $s}' |
    curl -s -X POST "$GATEWAY/v1/sessions/$session/statements" -H 'Content-Type: application/json' -d @- |
    jq -r '.operationHandle // empty')
  [[ -n "$op" ]] || die "SQL Gateway rejected statement: ${statement:0:80}"
  for _ in $(seq 120); do
    result=$(curl -s "$GATEWAY/v1/sessions/$session/operations/$op/result/0")
    if jq -e '.errors' <<<"$result" >/dev/null; then
      die "statement failed: ${statement:0:80}...
$(jq -r '.errors[-1]' <<<"$result" | grep -m3 -E 'Exception|Caused by' || jq -r '.errors[-1]' <<<"$result" | head -5)"
    fi
    if [[ "$(jq -r .resultType <<<"$result")" != NOT_READY ]]; then
      jq -r '.results.data[0].fields[0] // empty' <<<"$result"
      return
    fi
    sleep 0.5
  done
  die "timed out waiting for statement: ${statement:0:80}"
}

submit_sql() {
  local job=$1 file=jobs/sql/$1.sql session job_id=""
  (($# == 1)) || die "SQL jobs take no flags"
  session=$(curl -sf -X POST "$GATEWAY/v1/sessions" -H 'Content-Type: application/json' \
    -d "{\"sessionName\": \"$job\"}" | jq -r .sessionHandle)
  # Statements are separated by semicolons; comment lines are dropped.
  while IFS= read -r statement; do
    job_id=$(sql_statement "$session" "$statement")
  done < <(jq -Rrs 'split("\n") | map(select(test("^\\s*--") | not)) | join("\n")
                    | split(";") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0))
                    | map(gsub("\n"; " ")) | .[]' "$file")
  curl -sf -X DELETE "$GATEWAY/v1/sessions/$session" >/dev/null || true
  [[ -n "$job_id" ]] || die "$job: no job id returned"
  echo "$job_id"
}

submit_java() {
  local job=$1 jar_id job_id
  shift
  [[ -f "$JAR" ]] || die "$JAR not found; run 'make jobs' first"
  jar_id=$(curl -sf -X POST "$FLINK/jars/upload" -F "jarfile=@$JAR" | jq -r '.filename | split("/") | last')
  # Job flags start with "--", which jq would parse as its own options, so
  # they're passed to it on stdin rather than with --args.
  job_id=$(printf '%s\n' "--job=$job" "$@" | jq -R . |
    jq -s '{entryClass: "demo.platform.jobs.Main", programArgsList: .}' |
    curl -s -X POST "$FLINK/jars/$jar_id/run" -H 'Content-Type: application/json' -d @- |
    jq -r '.jobid // ("error: " + (.errors | tostring))')
  curl -sf -X DELETE "$FLINK/jars/$jar_id" >/dev/null || true
  [[ "$job_id" != error:* ]] || die "$job: ${job_id:0:500}"
  echo "$job_id"
}

submit() {
  local job=$1
  if [[ -f "jobs/sql/$job.sql" ]]; then
    submit_sql "$@"
  elif [[ " ${JAVA_JOBS[*]} " == *" $job "* ]]; then
    submit_java "$@"
  else
    die "unknown job '$job' (jobs: ${JAVA_JOBS[*]} ${SQL_JOBS[*]})"
  fi
}

# Prints "<job id> <name> <state>" for every job that hasn't terminated.
active_jobs() {
  curl -sf "$FLINK/jobs/overview" |
    jq -r '.jobs[] | select(.state | IN("FINISHED", "CANCELED", "FAILED") | not) | "\(.jid) \(.name) \(.state)"'
}

cancel() {
  local target=$1 jid name state found=0
  while read -r jid name state; do
    if [[ "$target" == all || "$name" == "$target" ]]; then
      curl -sf -X PATCH "$FLINK/jobs/$jid?mode=cancel" >/dev/null
      echo "cancelled $name ($jid)"
      found=1
    fi
  done < <(active_jobs)
  ((found)) || [[ "$target" == all ]] || die "no active job named '$target'"
}

case "${1:-}" in
  submit)
    (($# >= 2)) || die "usage: $0 submit <job> [flags...]"
    shift
    job_id=$(submit "$@")
    echo "$1: $job_id"
    ;;
  submit-all)
    for job in "${JAVA_JOBS[@]}" "${SQL_JOBS[@]}"; do
      job_id=$(submit "$job")
      echo "$job: $job_id"
    done
    ;;
  cancel)
    (($# == 2)) || die "usage: $0 cancel <job>|all"
    cancel "$2"
    ;;
  list)
    active_jobs | column -t
    ;;
  *)
    sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
