#!/usr/bin/env bash
# Bring the live stack up: everything in deploy/terraform, with the instance
# running. Terraform shows the plan and asks before it changes anything.
#
#   deploy/up.sh --image TAG --start 2026-10-01T00:00:00Z
#                     a dry run on the public development schedule
#   deploy/up.sh --image TAG --start 2026-10-01T00:00:00Z --schedule sealed
#                     the live window: needs docs/sealed-schedule.json and the
#                     secret in SSM at /verdict/schedule-secret
#   deploy/up.sh --no-run
#                     build it all, instance off (costs the volume only)
#
# TAG is what deploy/push-image.sh printed. The start is the window's, fixed
# for its whole length: a replacement instance continues the stream from it,
# so changing it mid-window starts a different stream.
#
# This starts the stack, not the live window's count. The live window does
# not start until the project cost allocation tag is active, because until
# then the verdict-monthly budget counts nothing (docs/STATE.md, section 6).
set -euo pipefail
cd "$(dirname "$0")/terraform"

running=true
image=""
start=""
schedule="dev"
while [ $# -gt 0 ]; do
  case "$1" in
    --no-run) running=false; shift ;;
    --image) image="${2:?--image needs the tag deploy/push-image.sh printed}"; shift 2 ;;
    --start) start="${2:?--start needs an RFC 3339 time}"; shift 2 ;;
    --schedule) schedule="${2:?--schedule is dev or sealed}"; shift 2 ;;
    *) echo "unknown option $1; see the top of this file" >&2; exit 2 ;;
  esac
done

# Terraform's provider cache is hundreds of megabytes; keep it out of a
# synced folder by pointing TF_DATA_DIR elsewhere (docs/STATE.md, section 2).
terraform init -input=false
terraform apply -var "running=$running" -var "image_tag=$image" \
  -var "window_start=$start" -var "schedule=$schedule"
