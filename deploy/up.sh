#!/usr/bin/env bash
# Bring the live stack up: everything in deploy/terraform, with the instance
# running. Terraform shows the plan and asks before it changes anything.
#
#   deploy/up.sh --image TAG   build it all and run that image (push-image.sh)
#   deploy/up.sh --no-run      build it all, instance off (costs the volume only)
#
# This starts the stack, not the live window. The window starts when the
# schedule is sealed, and not before the project cost allocation tag is
# active, because until then the verdict-monthly budget counts nothing
# (docs/STATE.md, section 6).
set -euo pipefail
cd "$(dirname "$0")/terraform"

running=true
image=""
case "${1:-}" in
  --no-run) running=false ;;
  --image) image="${2:?--image needs the tag deploy/push-image.sh printed}" ;;
  "") echo "deploy/up.sh --image TAG, or --no-run; see deploy/push-image.sh" >&2; exit 2 ;;
  *) echo "unknown option $1" >&2; exit 2 ;;
esac

# Terraform's provider cache is hundreds of megabytes; keep it out of a
# synced folder by pointing TF_DATA_DIR elsewhere (docs/STATE.md, section 2).
terraform init -input=false
terraform apply -var "running=$running" -var "image_tag=$image"
