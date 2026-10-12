# syntax=docker/dockerfile:1.7
#
# Multi-stage build for kinference. The image is mount-required: users
# pass their own config via -v + --config, e.g.
#
#   docker run --rm -p 50051:50051 \
#       -v $(pwd)/config.toml:/etc/kinference/config.toml:ro \
#       kinference --config /etc/kinference/config.toml
#
# Build:
#   DOCKER_BUILDKIT=1 docker build -t kinference .

# ---------- builder ----------
# Use a recent stable. The `dynamo-kv-router` git dep doesn't resolve under
# cargo 1.83 (workspace-inside-monorepo lookup changed), so we need >= 1.86.
# Keep this version in lockstep with rust-toolchain.toml. If they disagree,
# rustup silently downloads the pinned toolchain inside the container on every
# build — correct, but a slow surprise.
FROM rust:1.95-slim-bookworm AS builder

# `tonic-build` (build dep) needs protoc; the `zmq` crate links libzmq via
# pkg-config; the `dynamo-kv-router` git dep is fetched by cargo so git is
# required.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        g++ \
        pkg-config \
        protobuf-compiler \
        libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto ./proto
COPY src ./src
# Build-time version override. Passed in via `docker build --build-arg
# KINFERENCE_VERSION=<tag>`; consumed by `src/version.rs` via `option_env!`.
# Falls back to `CARGO_PKG_VERSION` when unset, so an unannotated build
# still has a usable version.
ARG KINFERENCE_VERSION
ENV KINFERENCE_VERSION=${KINFERENCE_VERSION}

# BuildKit cache mounts keep cargo's registry + the target dir warm across
# builds without bloating image layers. We copy the binaries out of the
# (non-persisted) target dir into /out so the runtime stage can find them.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bins \
    && mkdir -p /out \
    && cp target/release/kinference /out/ \
    && cp target/release/kinfctl /out/ \
    && cp target/release/compute_hash /out/

# ---------- runtime ----------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libzmq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin app

COPY --from=builder /out/kinference         /usr/local/bin/kinference
# The policy checker, so it runs with no Rust toolchain:
#   docker run --rm -i --entrypoint kinfctl <image> check - < policy.yaml
COPY --from=builder /out/kinfctl       /usr/local/bin/kinfctl
COPY --from=builder /out/compute_hash  /usr/local/bin/compute_hash

USER app
EXPOSE 50051

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/kinference"]
