# syntax=docker/dockerfile:1.7
#
# The image ships no config; mount one and pass --config, e.g.
#
#   docker run --rm -p 50051:50051 \
#       -v $(pwd)/config.json:/etc/kinference/config.json:ro \
#       kinference --config /etc/kinference/config.json

# Match rust-toolchain.toml, or rustup downloads that toolchain on every build.
FROM rust:1.95-slim-bookworm AS builder

# The `zmq` crate links libzmq via pkg-config; cargo needs git for the
# `dynamo-kv-router` dep.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        g++ \
        pkg-config \
        libzmq3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY src ./src
COPY crates ./crates

# Read by `option_env!` in both binaries; unset falls back to
# `CARGO_PKG_VERSION`.
ARG KINFERENCE_VERSION
ENV KINFERENCE_VERSION=${KINFERENCE_VERSION}

# The target dir is a cache mount, so the binaries are copied out to /out
# for the runtime stage.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --bins \
    && mkdir -p /out \
    && cp target/release/kinference /out/ \
    && cp target/release/kinfctl /out/ \
    && cp target/release/compute_hash /out/

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libzmq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin app

COPY --from=builder /out/kinference         /usr/local/bin/kinference
#   docker run --rm -i --entrypoint kinfctl <image> check - < policy.yaml
COPY --from=builder /out/kinfctl       /usr/local/bin/kinfctl
COPY --from=builder /out/compute_hash  /usr/local/bin/compute_hash

USER app
EXPOSE 50051

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/kinference"]
