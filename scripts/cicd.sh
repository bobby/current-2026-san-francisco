#!/usr/bin/env bash
# Talks to cicd-sim (docs/design.md §4.3), the platform's deployment pipeline.
#
#   scripts/cicd.sh catalogue                          versions per job (from jobs/versions.yaml)
#   scripts/cicd.sh deploy <job> <version> [args...]   e.g. deploy payments-enrichment 1.42.0
#   scripts/cicd.sh rollback <job> [version]           defaults to the previous version
#   scripts/cicd.sh baseline [job...]                  deploy baseline versions not already running
#
# Prints the platform.deployments event(s). TRIGGERED_BY (default user:$USER)
# says who asked.
set -euo pipefail

CICD=${CICD:-http://localhost:8091}
TRIGGERED_BY=${TRIGGERED_BY:-user:${USER:-unknown}}

die() { echo "error: $*" >&2; exit 1; }

post() {
  local path=$1 body=$2 response status
  response=$(curl -s -w '\n%{http_code}' -X POST "$CICD$path" -H 'Content-Type: application/json' -d "$body")
  status=${response##*$'\n'}
  jq . <<<"${response%$'\n'*}"
  [[ "$status" == 200 ]] || die "cicd-sim answered HTTP $status"
}

case "${1:-}" in
  catalogue)
    curl -sf "$CICD/catalogue" | jq -r '.jobs | to_entries[] | .key as $job | .value as $spec | .value.versions[] |
      [$job, .version, (if .version == $spec.baseline then "baseline" else "-" end), .summary, (.args | join(" ") | if . == "" then "-" else . end)] | @tsv' |
      column -t -s $'\t'
    ;;
  deploy)
    (($# >= 3)) || die "usage: $0 deploy <job> <version> [args...]"
    body=$(printf '%s\n' "${@:4}" | jq -R 'select(length > 0)' | jq -s --arg job "$2" --arg version "$3" \
      --arg by "$TRIGGERED_BY" '{job: $job, version: $version, args: ., triggered_by: $by}')
    post /deploy "$body"
    ;;
  rollback)
    (($# >= 2)) || die "usage: $0 rollback <job> [version]"
    post /rollback "$(jq -n --arg job "$2" --arg version "${3:-}" --arg by "$TRIGGERED_BY" \
      '{job: $job, triggered_by: $by} + (if $version == "" then {} else {to_version: $version} end)')"
    ;;
  baseline)
    shift
    post /baseline "$(jq -n --arg by "$TRIGGERED_BY" '{triggered_by: $by} + (if $ARGS.positional == [] then {} else {jobs: $ARGS.positional} end)' --args "$@")"
    ;;
  *)
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
