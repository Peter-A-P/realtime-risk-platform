#!/usr/bin/env bash
# Move the live window's record from the instance to this machine, before
# teardown destroys the data volume it lives on (ADR 36):
#
#   deploy/archive/fetch.sh --to DIR [--final]
#
# Three files go through the archive bucket, streamed from the volume so the
# instance needs no room for them, and come down under DIR/<when>/:
#
#   prometheus.tar  every metric since the window opened, the dashboard's data
#   kept.tar        history's kept sample, a parquet file and a manifest a day
#   state.tar       the window's start, the models job's state, the champion
#                   pointer, the scorer's starts, the spot notices, the alerts told
#
# --final stops Prometheus for its copy, so the copy is exactly what it had
# written; it is not started again, since teardown follows. Without it the
# copy is taken while Prometheus runs, and its last few minutes may need the
# write-ahead log repaired when the archive is served (deploy/archive/serve.sh
# lets Prometheus do that). The bucket's copies are removed once the files are
# down and their checksums match.
set -euo pipefail
cd "$(dirname "$0")/../terraform"

profile="${AWS_PROFILE:-verdict}"
region="ca-central-1"
export MSYS_NO_PATHCONV=1
to=""
final=0
while [ $# -gt 0 ]; do
  case "$1" in
    --to) to="${2:?--to needs a directory}"; shift 2 ;;
    --final) final=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$to" ] || { echo "usage: deploy/archive/fetch.sh --to DIR [--final]" >&2; exit 2; }

bucket=$(terraform output -raw archive_bucket)
instance=$(aws autoscaling describe-auto-scaling-groups --profile "$profile" --region "$region" \
  --auto-scaling-group-names "$(terraform output -raw autoscaling_group)" \
  --query 'AutoScalingGroups[0].Instances[0].InstanceId' --output text | tr -d '\r')
case "$instance" in i-*) ;; *) echo "no running instance: $instance" >&2; exit 1 ;; esac
when=$(date -u +%Y%m%dT%H%MZ)
prefix="s3://$bucket/$when"

# One shell script on the instance, through Session Manager; its output is
# printed when it ends. An hour is enough for the three streams.
on_instance() {
  local params id status
  params=$(mktemp)
  python -c 'import json,sys; print(json.dumps({"commands": [sys.argv[1]], "executionTimeout": ["3600"]}))' \
    "$1" > "$params"
  id=$(aws ssm send-command --profile "$profile" --region "$region" --instance-ids "$instance" \
    --document-name AWS-RunShellScript --parameters "file://$params" \
    --query Command.CommandId --output text | tr -d '\r')
  rm -f "$params"
  while :; do
    sleep 10
    status=$(aws ssm get-command-invocation --profile "$profile" --region "$region" \
      --command-id "$id" --instance-id "$instance" --query Status --output text 2>/dev/null \
      | tr -d '\r' || true)
    case "$status" in Pending|InProgress|Delayed|"") ;; *) break ;; esac
  done
  aws ssm get-command-invocation --profile "$profile" --region "$region" --command-id "$id" \
    --instance-id "$instance" --query '[StandardOutputContent,StandardErrorContent]' --output text
  [ "$status" = Success ]
}

stop_prometheus=""
[ "$final" = 1 ] && stop_prometheus="docker stop verdict-prometheus >/dev/null"
echo "streaming the record to $prefix"
on_instance "set -euo pipefail
$stop_prometheus
up() { aws s3 cp - '$prefix/'\$1 --region $region --checksum-algorithm SHA256 --only-show-errors; }
tar -C /data -cf - prometheus | up prometheus.tar
tar -C /data/history -cf - kept | up kept.tar
tar -C /data -cf - window-start models flags engine/starts.jsonl interruptions alerts | up state.tar
aws s3 ls '$prefix/' --region $region"

mkdir -p "$to/$when"
aws s3 cp "$prefix/" "$to/$when/" --recursive --profile "$profile" --region "$region" \
  --checksum-mode ENABLED --only-show-errors
for f in prometheus kept state; do
  tar -tf "$to/$when/$f.tar" >/dev/null
done
mkdir -p "$to/$when/data"
tar -C "$to/$when/data" -xf "$to/$when/prometheus.tar"
aws s3 rm "$prefix/" --recursive --profile "$profile" --region "$region" --only-show-errors
echo "archived to $to/$when; serve it with deploy/archive/serve.sh $to/$when"
