#!/usr/bin/env bash
# Build the image, tag it with the commit, and push it to the stack's ECR
# repository. Prints the tag to pass to deploy/up.sh.
#
#   deploy/push-image.sh
#
# Refuses a dirty working tree: the tag names a commit, and an image built
# from uncommitted code would carry a name that describes something else.
# The repository's tags are immutable, so a tag is pushed once.
set -euo pipefail
cd "$(dirname "$0")/.."

profile="${AWS_PROFILE:-verdict}"
region="ca-central-1"

if [ -n "$(git status --porcelain -- verdict pyproject.toml deploy/image)" ]; then
  echo "uncommitted changes under verdict/, pyproject.toml or deploy/image; commit first" >&2
  exit 1
fi
tag="$(git rev-parse --short=12 HEAD)"

repository=$(aws ecr describe-repositories --profile "$profile" --region "$region" \
  --repository-names verdict --query 'repositories[0].repositoryUri' --output text | tr -d '\r')
registry="${repository%%/*}"

aws ecr get-login-password --profile "$profile" --region "$region" \
  | docker login --username AWS --password-stdin "$registry"
docker build --platform linux/amd64 -f deploy/image/Dockerfile -t "$repository:$tag" .
docker push "$repository:$tag"
echo "$tag"
