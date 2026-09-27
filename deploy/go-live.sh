#!/usr/bin/env bash
# Start the live window: the sealed schedule, from a clean slate
# (docs/go-live.md has the checklist around this command).
#
#   deploy/go-live.sh --image TAG
#
# TAG is what deploy/push-image.sh printed. In order:
#
#   1. refuse unless the commitment is committed, the secret is in SSM and
#      the project cost allocation tag is active (docs/STATE.md, section 6);
#   2. choose the window's start, a quarter of an hour from now, which the
#      feeds wait for and which is fixed for the whole window;
#   3. apply the launch template for the sealed window (Terraform asks);
#   4. on the running instance, stop the stack and clear what the dry run
#      left: the scorer's saved state and starts, the feeds' saved places,
#      history, the models job's state, and the topics and their groups,
#      so nothing from the development stream reaches the live window's
#      features or its record. Prometheus, the champion pointer, the
#      alerts' state and the spot notices are kept;
#   5. replace the instance, so the new one boots on the new template;
#   6. wait for its boot to finish, and show the feed confirming the seal.
set -euo pipefail
cd "$(dirname "$0")"

profile="${AWS_PROFILE:-verdict}"
region="ca-central-1"
export MSYS_NO_PATHCONV=1
image=""
while [ $# -gt 0 ]; do
  case "$1" in
    --image) image="${2:?--image needs the tag deploy/push-image.sh printed}"; shift 2 ;;
    *) echo "unknown option $1; see the top of this file" >&2; exit 2 ;;
  esac
done
[ -n "$image" ] || { echo "--image is required" >&2; exit 2; }
aws_() { aws --profile "$profile" --region "$region" "$@"; }

# 1. Preconditions.
[ -f ../docs/sealed-schedule.json ] || { echo "no docs/sealed-schedule.json: seal first" >&2; exit 1; }
aws_ ssm describe-parameters --parameter-filters Key=Name,Values=/verdict/schedule-secret \
  --query 'Parameters[0].Name' --output text | grep -q schedule-secret \
  || { echo "no /verdict/schedule-secret in SSM" >&2; exit 1; }
aws --profile "$profile" ce list-cost-allocation-tags --tag-keys project --status Active \
  --query 'CostAllocationTags[0].TagKey' --output text | grep -q project \
  || { echo "the project cost allocation tag is not active" >&2; exit 1; }

# 2. The window's start.
start=$(date -u -d '+15 minutes' +%Y-%m-%dT%H:%M:00Z)
echo "the live window will start at $start"

# 3. The launch template for the sealed window.
(cd terraform && terraform init -input=false >/dev/null && terraform apply \
  -var running=true -var "image_tag=$image" -var "window_start=$start" -var schedule=sealed)

# 4. Clear the dry run's state on the running instance.
instance=$(aws_ autoscaling describe-auto-scaling-groups --auto-scaling-group-names verdict \
  --query 'AutoScalingGroups[0].Instances[0].InstanceId' --output text | tr -d '\r')
read -r -d '' clear <<'SCRIPT' || true
set -e
COMPOSE="docker compose -f /opt/verdict/compose.yml --env-file /etc/verdict/stack.env --env-file /etc/verdict/tunnel.env"
services=$($COMPOSE config --services | grep -v '^redpanda$' | grep -v '^topics$' | grep -v '^grafana-files$')
$COMPOSE stop $services
for topic in transactions labels decisions shadow dead-letter; do
  docker exec verdict-redpanda rpk topic delete "$topic" || true
done
for group in $(docker exec verdict-redpanda rpk group list | awk 'NR>1 {print $2}'); do
  docker exec verdict-redpanda rpk group delete "$group" || true
done
$COMPOSE stop redpanda
rm -rf /data/engine/* /data/feeds/* /data/history/* /data/models/*
sync
echo "cleared the dry run's state"
SCRIPT
params=$(mktemp)
python3 -c 'import json,sys; print(json.dumps({"commands": [sys.argv[1]], "executionTimeout": ["600"]}))' \
  "$clear" > "$params" 2>/dev/null \
  || python -c 'import json,sys; print(json.dumps({"commands": [sys.argv[1]], "executionTimeout": ["600"]}))' \
  "$clear" > "$params"
command=$(aws_ ssm send-command --instance-ids "$instance" --document-name AWS-RunShellScript \
  --parameters "file://$params" --query Command.CommandId --output text | tr -d '\r')
rm -f "$params"
for _ in $(seq 1 120); do
  status=$(aws_ ssm get-command-invocation --command-id "$command" --instance-id "$instance" \
    --query Status --output text 2>/dev/null | tr -d '\r' || true)
  case "$status" in Success) break ;; Failed|Cancelled|TimedOut) echo "clearing failed: $status" >&2; exit 1 ;; esac
  sleep 5
done
[ "$status" = Success ] || { echo "clearing did not finish" >&2; exit 1; }
echo "cleared $instance"

# 5. Replace the instance.
aws_ autoscaling terminate-instance-in-auto-scaling-group --instance-id "$instance" \
  --no-should-decrement-desired-capacity --query Activity.Description --output text

# 6. Wait for the new instance's boot, then show the feed.
new=""
for _ in $(seq 1 60); do
  new=$(aws_ autoscaling describe-auto-scaling-groups --auto-scaling-group-names verdict \
    --query "AutoScalingGroups[0].Instances[?InstanceId!='$instance'].InstanceId | [0]" \
    --output text | tr -d '\r')
  [ -n "$new" ] && [ "$new" != None ] && break
  sleep 10
done
echo "new instance $new; waiting for its boot"
for _ in $(seq 1 60); do
  command=$(aws_ ssm send-command --instance-ids "$new" --document-name AWS-RunShellScript \
    --parameters 'commands=["grep -c \"boot finished\" /var/log/verdict-boot.log && docker logs --tail 5 verdict-feed-transactions"]' \
    --query Command.CommandId --output text 2>/dev/null | tr -d '\r' || true)
  sleep 10
  out=$(aws_ ssm get-command-invocation --command-id "$command" --instance-id "$new" \
    --query StandardOutputContent --output text 2>/dev/null || true)
  case "$out" in 1*) echo "$out"; break ;; esac
  sleep 20
done
echo "the live window starts at $start and runs sixty days; the secret is revealed the day after"
