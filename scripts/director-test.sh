#!/usr/bin/env bash
# The director's done-when check (issue #7): two runs of
# `director reset && director run --scenario <scenario>` with the same seed
# produce the same sequence of incidents. Run it with the stack up:
#
#   scripts/director-test.sh [all|1|2|3]     (default all; about an hour)
#
# It also checks that the timeline's starring incidents happened: F2's poison
# records fail trade-events-ingest, F1 fails payments-enrichment, and F3
# backs up market-data-to-iceberg.
set -euo pipefail

cd "$(dirname "$0")/.."

SCENARIO=${1:-all}
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }
step() { printf '\n%s\n' "$*"; }

# On the EC2 instance the director is prebuilt (bin/director).
[[ -x bin/director ]] || cargo build --quiet --release -p director
director() { scripts/director.sh "$@"; }

for i in 1 2; do
  step "Run $i: reset, then scenario $SCENARIO"
  director reset
  director run --scenario "$SCENARIO" | tee "$OUT/run$i.log"
  director incidents >"$OUT/incidents$i"
  director incidents --times >"$OUT/timed$i"
  [[ -s "$OUT/incidents$i" ]] || fail "run $i produced no incidents"
  pass "run $i: $(wc -l <"$OUT/incidents$i" | tr -d ' ') incidents"
done

step "Starring incidents"
expect() {  # <scenarios> <pattern> <what>
  [[ " $1 " == *" $SCENARIO "* ]] || return 0
  grep -q -- "$2" "$OUT/incidents1" || fail "$3 is missing: $(paste -sd ';' "$OUT/incidents1")"
  pass "$3: $(grep -- "$2" "$OUT/incidents1")"
}
expect "all 2" '^trade-events-ingest failure' "F2 (poison records)"
expect "all 3" '^payments-enrichment failure: NullPointerException' "F1 (bad deploy)"
expect "all 3" '^market-data-to-iceberg alert' "F3 (traffic spike)"

step "Same seed, same incidents"
if diff -u "$OUT/incidents1" "$OUT/incidents2" >"$OUT/diff"; then
  pass "both runs produced the same $(wc -l <"$OUT/incidents1" | tr -d ' ') incidents in the same order:"
  paste "$OUT/timed1" "$OUT/timed2" | awk -F'\t' '{split($2, b, "  "); printf "      %s   (run 2: %s)\n", $1, b[1]}'
else
  cat "$OUT/diff" >&2
  fail "the runs' incidents differ"
fi

printf '\n\033[32mDirector test passed.\033[0m\n'
