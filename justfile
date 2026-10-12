set shell := ["bash", "-eo", "pipefail", "-c"]

IMAGE_REPO := "kinference"
IMAGE_TAG := "latest"
IMAGE := IMAGE_REPO + ":" + IMAGE_TAG
# Baked into the binary via `option_env!` in src/version.rs; defaults to the
# image tag so `kinference --version` identifies the build it came from.
KINFERENCE_VERSION := IMAGE_TAG

default: list

list:
    @just --list

[group('check')]
check-fmt:
    cargo fmt --all -- --check

[group('check')]
clippy:
    cargo clippy --all-targets -- -D warnings

[group('check')]
lint-chart:
    helm lint charts/kinference-crds charts/kinference
    helm template charts/kinference-crds > /dev/null
    helm template charts/kinference > /dev/null

[group('check')]
lint: check-fmt clippy lint-chart

[group('test')]
test-unit:
    cargo test

[group('build')]
image:
    docker build --build-arg KINFERENCE_VERSION={{KINFERENCE_VERSION}} -t {{IMAGE}} .

# install `kinfctl`, the InferencePolicy checker, into ~/.cargo/bin
[group('build')]
install-cli:
    cargo install --path . --bin kinfctl --locked

# `cli-bin` builds `kinfctl` for one rust target and lays it out the way
# the release workflow attaches it to the GitHub release:
#   target/cli/kinfctl-<version>-<os>-<arch>   (+ a `.sha256` sidecar)
# <version> is KINFERENCE_VERSION, which is also what the binary's `--version`
# reports. The target defaults to the host; the release workflow passes each
# matrix target explicitly. `--target` is always given so the binary lands at
# the same path whether or not the target is the host.

# build `kinfctl` for one target into target/cli/kinfctl-<version>-<os>-<arch> (+ .sha256)
[group('build')]
cli-bin target="":
    #!/usr/bin/env bash
    set -euo pipefail
    target='{{target}}'
    target=${target:-$(rustc -vV | sed -n 's/^host: //p')}
    KINFERENCE_VERSION='{{KINFERENCE_VERSION}}' \
        cargo build --release --locked --bin kinfctl --target "$target"
    case $target in
      aarch64-apple-darwin)      os_arch=darwin-arm64 ;;
      x86_64-apple-darwin)       os_arch=darwin-amd64 ;;
      x86_64-unknown-linux-gnu)  os_arch=linux-amd64 ;;
      aarch64-unknown-linux-gnu) os_arch=linux-arm64 ;;
      *) echo "cli-bin: no asset name for target $target" >&2; exit 1 ;;
    esac
    mkdir -p target/cli
    asset="kinfctl-{{KINFERENCE_VERSION}}-$os_arch"
    cp "target/$target/release/kinfctl" "target/cli/$asset"
    (cd target/cli && shasum -a 256 "$asset" > "$asset.sha256")
    echo "target/cli/$asset"

CLUSTER := "kinference-smoke"

# Build the image, stand up a kind cluster, install both charts, and wait
# for kinference to go Available. Override the image coordinates the same way
# CI and the release workflow do: `just IMAGE_REPO=... IMAGE_TAG=... up`.
[group('e2e')]
up: image
    kind create cluster --name {{CLUSTER}}
    kind load docker-image {{IMAGE}} --name {{CLUSTER}}
    helm install kinference-crds charts/kinference-crds --kube-context kind-{{CLUSTER}}
    helm install kinference charts/kinference --kube-context kind-{{CLUSTER}} \
        --set image.repository={{IMAGE_REPO}} --set image.tag={{IMAGE_TAG}}
    kubectl --context kind-{{CLUSTER}} wait --for=condition=available deployment/kinference --timeout=180s

# Deploy the test fixtures from tests/e2e/manifests — the REAL dummy-vllm
# (CPU, dummy weights, image pinned by digest in the manifest) + a workerset
# and pool — then assert the installed chart FUNCTIONS against the real
# engine: discovery, KV ingest (triggered by an actual completion request),
# CRD status. See tests/e2e/smoke.sh.
[group('e2e')]
test-smoke:
    kubectl --context kind-{{CLUSTER}} apply -f tests/e2e/manifests
    KIND_CONTEXT=kind-{{CLUSTER}} tests/e2e/smoke.sh

[group('e2e')]
down:
    kind delete cluster --name {{CLUSTER}}
