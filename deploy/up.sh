#!/usr/bin/env bash
# Bring the live stack up: everything in deploy/terraform, with the instance
# running. Terraform shows the plan and asks before it changes anything.
#
#   deploy/up.sh             build it all and start the instance
#   deploy/up.sh --no-run    build it all, instance off (costs the volume only)
#
# This starts the stack, not the live window. The window starts when the
# schedule is sealed, and not before the project cost allocation tag is
# active, because until then the verdict-monthly budget counts nothing
# (docs/STATE.md, section 6).
set -euo pipefail
cd "$(dirname "$0")/terraform"

running=true
if [ "${1:-}" = "--no-run" ]; then
  running=false
fi

# Terraform's provider cache is hundreds of megabytes; keep it out of a
# synced folder by pointing TF_DATA_DIR elsewhere (docs/STATE.md, section 2).
terraform init -input=false
terraform apply -var "running=$running"
