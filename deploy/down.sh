#!/usr/bin/env bash
# Tear the live stack down, then prove it: ask AWS, not the Terraform state,
# for everything still tagged project=verdict. Teardown is part of done, and
# state can be lost or wrong.
#
#   deploy/down.sh          destroy, then check
#   deploy/down.sh --check  only check
#
# Exits non-zero if anything tagged remains beyond what is meant to outlive
# the stack: the tunnel token parameter, which is free and put there by hand.
set -euo pipefail
cd "$(dirname "$0")/terraform"

profile="${AWS_PROFILE:-verdict}"
region="ca-central-1"

if [ "${1:-}" != "--check" ]; then
  terraform init -input=false
  terraform destroy
fi

# MSYS_NO_PATHCONV stops Git Bash on Windows rewriting /verdict/ into a path.
left=$(MSYS_NO_PATHCONV=1 aws resourcegroupstaggingapi get-resources \
  --profile "$profile" --region "$region" \
  --tag-filters Key=project,Values=verdict \
  --query 'ResourceTagMappingList[].ResourceARN' --output text \
  | tr '\t' '\n' | tr -d '\r' \
  | grep -v ':parameter/verdict/cloudflare-tunnel-token$' | grep -v '^$' || true)

if [ -n "$left" ]; then
  echo "still tagged project=verdict after teardown:"
  echo "$left"
  exit 1
fi
echo "nothing tagged project=verdict remains but the tunnel token parameter"
