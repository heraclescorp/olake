FROM --platform=$BUILDPLATFORM golang:1.26.10-bookworm AS makefiles
WORKDIR /home/app
COPY . .
RUN mkdir /out && cp --parents Makefile $(find drivers -maxdepth 2 -name driver.mk) /out

# Hive's native GSSAPI transport requires a target-platform C compiler and headers.
FROM golang:1.26.10-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends libkrb5-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /home/app
ARG DRIVER_NAME=olake
ARG TARGETARCH
COPY --from=makefiles /out/ .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make prepare.${DRIVER_NAME} OVERLAY_DIR=/runtime-overlay
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOARCH=${TARGETARCH} make dev.${DRIVER_NAME}.build OVERLAY_DIR=/runtime-overlay

FROM ubuntu:24.04 AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libgssapi-krb5-2 libxml2 libpam-modules libcrypt1 iproute2 lsof \
    && find /usr/lib /lib -name libgssapi_krb5.so.2 -execdir /usr/bin/ln -sf libgssapi_krb5.so.2 libgssapi_krb5.so \; \
    && rm -rf /var/lib/apt/lists/*

FROM runtime-base
ARG DRIVER_VERSION=dev
ARG DRIVER_NAME=olake
COPY --from=builder /home/app/drivers/${DRIVER_NAME}/olake /home/olake
ENV DRIVER_VERSION=${DRIVER_VERSION}
COPY --from=builder /home/app/drivers/${DRIVER_NAME}/resources/spec.json /drivers/${DRIVER_NAME}/resources/spec.json
COPY --from=builder /home/app/destination/iceberg/resources/spec.json /destination/iceberg/resources/spec.json
COPY --from=builder /home/app/destination/parquet/resources/spec.json /destination/parquet/resources/spec.json
COPY --from=builder /runtime-overlay/ /
RUN ldconfig
LABEL io.eggwhite.version=${DRIVER_VERSION}
LABEL io.eggwhite.name=olake/source-${DRIVER_NAME}
WORKDIR /home
ENTRYPOINT ["./olake"]
