#!/usr/bin/env bash
# The cloud runtime (issue #3; docs/design.md §4.5): one EC2 instance running
# compose.yml + compose.aws.yml, with S3 and Glue. Wrapped by the *-aws make
# targets.
#
#   scripts/aws.sh stack              create or update the CloudFormation stack
#   scripts/aws.sh secrets            copy local secrets to SSM Parameter Store
#   scripts/aws.sh deploy             push images, upload the Compose bundle, start it
#   scripts/aws.sh exec <command>     run a shell command in /opt/platform on the instance
#   scripts/aws.sh jobs <args...>     run scripts/jobs.sh on the instance
#   scripts/aws.sh tunnel             forward the UIs to localhost (needs session-manager-plugin)
#   scripts/aws.sh shell              open a shell on the instance
#   scripts/aws.sh stop|start         stop or start the instance (data is kept)
#   scripts/aws.sh teardown           delete the stack, its data and the secrets
#
# AWS_PROFILE defaults to evidentsystems, AWS_REGION to us-east-1 and
# AWS_STACK to current2026-demo.
set -euo pipefail

cd "$(dirname "$0")/.."

export AWS_PROFILE=${AWS_PROFILE:-evidentsystems}
export AWS_REGION=${AWS_REGION:-us-east-1}
STACK=${AWS_STACK:-current2026-demo}
TEMPLATE=infra/cloudformation/demo.yaml
REMOTE_DIR=/opt/platform
LOCAL_FLINK_IMAGE=platform/flink:2.1.2
# Secrets read from the local .env, if set there, and written to the
# instance's .env. The ShadowTraffic license is stored whole.
ENV_SECRETS=(JEV_API_KEY ANTHROPIC_API_KEY MATTERMOST_BOT_TOKEN)
LICENSE_FILE=shadowtraffic/license.env
# Ports forwarded by `tunnel`. Add Grafana, Mattermost and the tickets UI as
# they land.
TUNNEL_PORTS=(8081 8083 9090)
# Files the instance needs to run the stack and the jobs.
BUNDLE=(
  compose.yml compose.aws.yml
  flink/catalogs/aws
  jobs/sql jobs/java/target/platform-jobs.jar
  observability postgres
  shadowtraffic/inputs.json
  scripts
)

die() { echo "error: $*" >&2; exit 1; }
log() { printf '\033[1m==> %s\033[0m\n' "$*" >&2; }

output() {
  aws cloudformation describe-stacks --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text 2>/dev/null ||
    die "stack $STACK not found; run 'make stack-aws' first"
}

instance_id() { output InstanceId; }

# Waits until the instance's SSM agent is online.
wait_for_ssm() {
  local id=$1
  for _ in $(seq 60); do
    [[ "$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$id" \
      --query 'InstanceInformationList[0].PingStatus' --output text)" == Online ]] && return
    sleep 5
  done
  die "instance $id is not online in SSM"
}

# remote <script>: runs a bash script as root on the instance through SSM Run
# Command, prints its output and exits with its status.
remote() {
  local id cmd status params
  id=$(instance_id)
  params=$(mktemp)
  jq -n --arg script "$1" '{commands: ["set -euo pipefail", $script], executionTimeout: ["3600"]}' >"$params"
  # Explicit checks, since callers may run this where `set -e` is off.
  cmd=$(aws ssm send-command --instance-ids "$id" --document-name AWS-RunShellScript \
    --parameters "file://$params" --query Command.CommandId --output text) || cmd=""
  rm -f "$params"
  [[ -n "$cmd" ]] || die "could not send a command to $id"
  # The invocation can take a few seconds to appear after send-command.
  local tries=0
  while :; do
    status=$(aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
      --query Status --output text 2>/dev/null) || { ((++tries < 20)) || die "lost command $cmd"; status=Pending; }
    case "$status" in Pending | InProgress | Delayed) sleep 3 ;; *) break ;; esac
  done
  aws ssm get-command-invocation --command-id "$cmd" --instance-id "$id" \
    --query '[StandardOutputContent, StandardErrorContent]' --output text
  [[ "$status" == Success ]] || die "remote command $status"
}

stack() {
  log "Deploying stack $STACK"
  aws cloudformation deploy --template-file "$TEMPLATE" --stack-name "$STACK" \
    --capabilities CAPABILITY_IAM --no-fail-on-empty-changeset
  log "Waiting for the instance to come online in SSM"
  wait_for_ssm "$(instance_id)"
  aws cloudformation describe-stacks --stack-name "$STACK" --query 'Stacks[0].Outputs' --output table
}

# put_secret <name> <file>: stores a file's contents as a SecureString. The
# value is passed as a file so it never appears in a process list or log.
put_secret() {
  aws ssm put-parameter --name "/$STACK/$1" --type SecureString --overwrite \
    --value "file://$2" >/dev/null
  echo "stored /$STACK/$1"
}

secrets() {
  [[ -f "$LICENSE_FILE" ]] || die "$LICENSE_FILE is missing (see $LICENSE_FILE.example)"
  put_secret shadowtraffic/license.env "$LICENSE_FILE"
  local name value tmp
  tmp=$(mktemp)
  trap 'rm -f "$tmp"' RETURN
  for name in "${ENV_SECRETS[@]}"; do
    value=$( (grep -E "^$name=" .env 2>/dev/null || true) | tail -1 | cut -d= -f2-)
    if [[ -n "$value" ]]; then
      printf '%s' "$value" >"$tmp"
      put_secret "env/$name" "$tmp"
    else
      echo "skipped $name (not set in .env)"
    fi
  done
}

deploy() {
  local bucket registry repo key image
  bucket=$(output BucketName)
  registry=$(output Registry)
  repo=$(output FlinkRepositoryUri)
  image=$repo:${LOCAL_FLINK_IMAGE##*:}
  [[ -f jobs/java/target/platform-jobs.jar ]] || die "jar not built; run 'make jobs'"

  log "Building and pushing $image"
  DOCKER_DEFAULT_PLATFORM=linux/arm64 docker compose build jobmanager
  docker tag "$LOCAL_FLINK_IMAGE" "$image"
  aws ecr get-login-password | docker login --username AWS --password-stdin "$registry" >/dev/null
  docker push --quiet "$image"

  log "Uploading the Compose bundle"
  key=bundles/$(date -u +%Y%m%dT%H%M%SZ).tgz
  COPYFILE_DISABLE=1 tar czf - "${BUNDLE[@]}" | aws s3 cp --quiet - "s3://$bucket/$key"

  log "Starting the stack on $(instance_id)"
  wait_for_ssm "$(instance_id)"
  remote "$(cat <<EOF
cloud-init status --wait >/dev/null
mkdir -p $REMOTE_DIR && cd $REMOTE_DIR
aws s3 cp --quiet s3://$bucket/$key /tmp/bundle.tgz
tar xzf /tmp/bundle.tgz && rm /tmp/bundle.tgz
umask 077
aws ssm get-parameter --name /$STACK/shadowtraffic/license.env --with-decryption \
  --query Parameter.Value --output text >shadowtraffic/license.env ||
  { echo "ShadowTraffic license missing from SSM; run 'make secrets-aws'"; exit 1; }
{
  echo COMPOSE_FILE=compose.yml:compose.aws.yml
  echo AWS_REGION=$AWS_REGION
  echo S3_BUCKET=$bucket
  echo FLINK_IMAGE=$image
  aws ssm get-parameters-by-path --path /$STACK/env/ --with-decryption \
    --query 'Parameters[].[Name, Value]' --output text |
    awk -F '\t' '{ sub(/.*\//, "", \$1); print \$1 "=" \$2 }'
} >.env
aws ecr get-login-password | docker login --username AWS --password-stdin $registry >/dev/null
docker compose pull --quiet
docker compose up -d --wait --remove-orphans
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
EOF
)"
}

tunnel() {
  command -v session-manager-plugin >/dev/null ||
    die "install the Session Manager plugin first: brew install --cask session-manager-plugin"
  local id port
  id=$(instance_id)
  trap 'kill $(jobs -p) 2>/dev/null' EXIT
  for port in "${TUNNEL_PORTS[@]}"; do
    aws ssm start-session --target "$id" --document-name AWS-StartPortForwardingSession \
      --parameters "portNumber=$port,localPortNumber=$port" >/dev/null &
  done
  echo "Forwarding localhost ports ${TUNNEL_PORTS[*]} to $id. Ctrl-C to stop."
  wait
}

teardown() {
  local bucket
  bucket=$(output BucketName)
  # Stop the jobs first, or they keep writing to the bucket while it's emptied.
  log "Stopping the stack on the instance"
  (remote "cd $REMOTE_DIR && docker compose down") >/dev/null 2>&1 ||
    aws ec2 stop-instances --instance-ids "$(instance_id)" >/dev/null 2>&1 || true
  log "Emptying s3://$bucket"
  aws s3 rm --quiet --recursive "s3://$bucket"
  log "Deleting stack $STACK"
  aws cloudformation delete-stack --stack-name "$STACK"
  aws cloudformation wait stack-delete-complete --stack-name "$STACK"
  log "Deleting secrets under /$STACK/"
  local names
  names=$(aws ssm get-parameters-by-path --path "/$STACK/" --recursive --query 'Parameters[].Name' --output text)
  [[ -z "$names" ]] || tr '\t' '\n' <<<"$names" |
    xargs -n 10 aws ssm delete-parameters --query DeletedParameters --output text --names
}

case "${1:-}" in
  stack) stack ;;
  secrets) secrets ;;
  deploy) deploy ;;
  exec)
    shift
    (($#)) || die "usage: $0 exec <command>"
    remote "cd $REMOTE_DIR && $*"
    ;;
  jobs)
    shift
    remote "cd $REMOTE_DIR && scripts/jobs.sh $(printf '%q ' "$@")"
    ;;
  tunnel) tunnel ;;
  shell) aws ssm start-session --target "$(instance_id)" ;;
  stop) aws ec2 stop-instances --instance-ids "$(instance_id)" --query 'StoppingInstances[0].CurrentState.Name' --output text ;;
  start)
    aws ec2 start-instances --instance-ids "$(instance_id)" --query 'StartingInstances[0].CurrentState.Name' --output text
    wait_for_ssm "$(instance_id)"
    remote "cd $REMOTE_DIR && docker compose up -d --wait"
    ;;
  teardown) teardown ;;
  *)
    sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
