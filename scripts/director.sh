#!/usr/bin/env bash
# Runs the director (docs/design.md §10.2) from the platform directory:
#
#   scripts/director.sh reset
#   scripts/director.sh run [--scenario 1|2|3|all]
#   scripts/director.sh status
#   scripts/director.sh incidents [--times]
#   scripts/director.sh inject <fault>
#
# Locally it builds and runs it with cargo. On the EC2 instance (where
# `make deploy-aws` puts SERVICES_IMAGE in .env) it runs the director from the
# services image, which is built against a newer glibc than Amazon Linux's:
# in a container on the host's network, with the host's Docker CLI, Compose
# plugin and socket, and the platform directory at the same path, so Compose
# resolves its bind mounts on the host.
set -euo pipefail

cd "$(dirname "$0")/.."

services_image=$( (grep -E '^SERVICES_IMAGE=' .env 2>/dev/null || true) | tail -1 | cut -d= -f2-)
if [[ -n "$services_image" ]]; then
  exec docker run --rm --network host --user root \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$(command -v docker)":/usr/bin/docker:ro \
    -v /usr/local/lib/docker/cli-plugins:/usr/local/lib/docker/cli-plugins:ro \
    -v "$PWD:$PWD" -w "$PWD" \
    "$services_image" director "$@"
fi
exec cargo run --quiet --release -p director -- "$@"
