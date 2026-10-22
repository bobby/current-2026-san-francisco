#!/usr/bin/env bash
# Runs the director (docs/design.md §10.2) from the platform directory:
#
#   scripts/director.sh reset
#   scripts/director.sh run [--scenario 1|2|3|all]
#   scripts/director.sh status
#   scripts/director.sh incidents [--times]
#   scripts/director.sh inject <fault>
#
# On the EC2 instance it uses bin/director, which `make deploy-aws` extracts
# from the services image; locally it builds and runs it with cargo.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -x bin/director ]]; then
  exec bin/director "$@"
fi
exec cargo run --quiet --release -p director -- "$@"
