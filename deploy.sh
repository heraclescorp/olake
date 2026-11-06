#!/bin/bash
set -euo pipefail
cd "${0%/*}"

REGISTRY="199658938451.dkr.ecr.us-east-2.amazonaws.com"
IMAGE="$REGISTRY/olakego/source-postgres:latest"

aws ecr get-login-password --region us-east-2 |
  docker login --username AWS --password-stdin "$REGISTRY"

docker build --platform linux/amd64 \
  --build-arg DRIVER_NAME=postgres \
  --tag "$IMAGE" .

docker push "$IMAGE"
