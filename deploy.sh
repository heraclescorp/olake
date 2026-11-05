#!/bin/bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

# Usage: ./deploy.sh <versioned-image-tag>
IMAGE_TAG="${1:?Usage: ./deploy.sh <versioned-image-tag>}"
if [[ "$IMAGE_TAG" == "latest" || "$IMAGE_TAG" == "cache" ]]; then
  echo "Use a versioned image tag instead of latest or cache" >&2
  exit 1
fi

REGISTRY="199658938451.dkr.ecr.us-east-2.amazonaws.com"
# Repository is provisioned separately.
POSTGRES_IMAGE="$REGISTRY/olakego/source-postgres"
IMAGE_NAME="$POSTGRES_IMAGE:$IMAGE_TAG"
read -r -a AWS_ARGS <<< "--region us-east-2 ${HERACLES_AWS_PROFILE:-}"

echo "Logging into AWS ECR"
aws ecr get-login-password "${AWS_ARGS[@]}" | \
  docker login --username AWS --password-stdin "$REGISTRY"

make iceberg.jar
PLATFORMS="$(make --no-print-directory print.platforms.postgres)"

if ! docker buildx inspect crossplat >/dev/null 2>&1; then
  docker buildx create --name=crossplat --node=crossplat
fi

echo "Building and pushing $IMAGE_NAME"
docker buildx build \
  --builder crossplat \
  --push \
  --platform "$PLATFORMS" \
  --build-arg DRIVER_NAME=postgres \
  --build-arg "DRIVER_VERSION=$IMAGE_TAG" \
  --cache-to "mode=max,image-manifest=true,oci-mediatypes=true,type=registry,ref=$POSTGRES_IMAGE:cache,ignore-error=true,timeout=2m" \
  --cache-from "type=registry,ref=$POSTGRES_IMAGE:cache" \
  --tag "$IMAGE_NAME" \
  --file Dockerfile .
