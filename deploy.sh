#!/bin/bash
set -euo pipefail
cd "${0%/*}"

REGISTRY="199658938451.dkr.ecr.us-east-2.amazonaws.com"
IMAGE="$REGISTRY/olake:latest"
BUILDER="olake"
DRIVER_VERSION=$(git describe --tags --match 'v[0-9]*' --abbrev=0)

printf 'Logging into AWS ECR\n'
aws ecr get-login-password --region us-east-2 |
  docker login --username AWS --password-stdin "$REGISTRY"

if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
  docker buildx create --name "$BUILDER" --driver docker-container
fi

printf 'Building the Iceberg writer JAR\n'
docker buildx build \
  --builder "$BUILDER" \
  --file - \
  --output type=local,dest=destination/iceberg/olake-iceberg-java-writer/target \
  . <<'DOCKERFILE'
FROM maven:3.9-eclipse-temurin-17 AS writer
WORKDIR /build
COPY destination/iceberg/olake-iceberg-java-writer/pom.xml ./
COPY destination/iceberg/olake-iceberg-java-writer/src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B clean package -Dmaven.test.skip=true
FROM scratch
COPY --from=writer /build/target/olake-iceberg-java-writer-0.0.1-SNAPSHOT.jar /
DOCKERFILE

printf 'Building and pushing %s\n' "$IMAGE"
docker buildx build \
  --builder "$BUILDER" \
  --platform linux/amd64 \
  --build-arg DRIVER_NAME=postgres \
  --build-arg "DRIVER_VERSION=$DRIVER_VERSION" \
  --tag "$IMAGE" \
  --push \
  .

printf 'Published %s\n' "$IMAGE"
