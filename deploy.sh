#!/bin/bash
set -euo pipefail
cd "${0%/*}"

REGISTRY="199658938451.dkr.ecr.us-east-2.amazonaws.com"
IMAGE="$REGISTRY/olakego/source-postgres"
VERSION=$(git describe --tags --match 'v[0-9]*' --abbrev=0)

aws ecr get-login-password --region us-east-2 |
  docker login --username AWS --password-stdin "$REGISTRY"

docker build --platform linux/amd64 \
  --build-arg DRIVER_NAME=postgres \
  --tag "$IMAGE:$VERSION" \
  --tag "$IMAGE:latest" .

docker push "$IMAGE:$VERSION"
docker push "$IMAGE:latest"
