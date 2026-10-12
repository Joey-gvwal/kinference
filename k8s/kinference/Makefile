# Makefile — kinference image build, push, and deploy.
#
# TWO TARGETS. `TARGET=aws` (the default) builds linux/amd64, pushes to ECR, and
# deploys to the harness cluster. `TARGET=local` builds for the host
# architecture straight into the local Docker daemon that Docker Desktop's
# Kubernetes shares, so there is no registry and `push` is a no-op.
#
# AWS:
#
#   make ecr-login                                            # 12-hour token
#   TAG=$$(git rev-parse --short HEAD)
#   make build push deploy TAG=$$TAG                          # full round-trip
#
# Docker Desktop:
#
#   make local-setup                                          # once per cluster
#   make build push deploy TARGET=local TAG=dev
#
# `make crds` is separate from `deploy` on BOTH targets: CRDs are cluster-scoped
# and outlive any one deployment, so bundling them would make a routine redeploy
# a schema change. `local-setup` is that plus the one thing a laptop needs — the
# Deployment pins `nodeSelector: role=system` to keep kinference off GPU-tainted
# nodes, and a single unlabelled local node would otherwise leave the pod
# Pending with a FailedScheduling event and nothing else wrong.
#
# Or run individual stages when iterating:
#
#   make build  TAG=$$TAG                                     # just build
#   make push   TAG=$$TAG                                     # just push
#   make deploy TAG=$$TAG                                     # just deploy (image tag overridden at apply time)
#   make undeploy                                             # tear it down
#
# `deploy` substitutes the image tag at render time, so deployment.yaml
# on disk stays a clean placeholder — no risk of accidentally committing
# a pinned tag.

# === EDIT ME ===========================================================
# Your ECR registry URL. Format: <aws-account-id>.dkr.ecr.<region>.amazonaws.com
# e.g. 471112539178.dkr.ecr.us-west-2.amazonaws.com
ECR_REGISTRY := 471112539178.dkr.ecr.us-west-2.amazonaws.com
# =======================================================================

ECR_REPO   := kinference
AWS_REGION := us-west-2

# Where this deploys. `aws` | `local`.
TARGET ?= aws

# Resolve the repo root from this file's location so the Makefile works
# regardless of where `make` is invoked from.
REPO_ROOT     := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
DASHBOARD_DIR := $(REPO_ROOT)/dashboard
MANIFEST_DIR  := $(dir $(lastword $(MAKEFILE_LIST)))

ifeq ($(TARGET),local)

# No registry. Docker Desktop's Kubernetes reads the same image store the
# local `docker build` writes to, so a bare name resolves and `push` has
# nothing to do. deployment.yaml already sets `imagePullPolicy: IfNotPresent`,
# which is what stops the kubelet trying to fetch it from a registry that does
# not exist.
IMAGE    := $(ECR_REPO)
# The host's architecture, NOT linux/amd64. Docker Desktop's Kubernetes node is
# the VM's arch — arm64 on Apple Silicon — and an amd64 image lands there as a
# CrashLoopBackOff with `exec format error`, since the binfmt emulation that
# makes `docker run` work is not in the kubelet's path.
LOCAL_ARCH := $(shell uname -m | sed -e 's/^aarch64$$/arm64/' -e 's/^x86_64$$/amd64/')
PLATFORM := linux/$(LOCAL_ARCH)
# Context only — deliberately NOT --kubeconfig. The AWS path points at one
# specific file the harness writes, but a laptop's config is whatever kubectl
# normally resolves, including a merged KUBECONFIG of several files. Pinning
# $(HOME)/.kube/config would silently drop the rest of that merge and fail on a
# context that plainly works from the shell.
KUBE_CONTEXT ?= docker-desktop
KUBECTL      := kubectl --context=$(KUBE_CONTEXT)
# Named in the `crds` output: it is cluster-scoped, so which cluster matters.
CLUSTER_LABEL := context $(KUBE_CONTEXT)

else

IMAGE    := $(ECR_REGISTRY)/$(ECR_REPO)
PLATFORM := linux/amd64
# kubectl always against the harness's cluster-specific kubeconfig, which
# the parent Makefile writes at ~/.kube/$(CLUSTER_NAME) via `make kubeconfig`.
# Track CLUSTER_NAME (same default as the parent) so this never drifts to a
# torn-down cluster; override to target another, e.g.
#   make deploy CLUSTER_NAME=wm-inf-31b-v2 TAG=...
CLUSTER_NAME    ?= inf-testbed
KUBECONFIG_PATH ?= $(HOME)/.kube/$(CLUSTER_NAME)
KUBECTL         := kubectl --kubeconfig=$(KUBECONFIG_PATH)
CLUSTER_LABEL   := cluster $(CLUSTER_NAME)

endif

# Placeholder string in deployment.yaml that `deploy` overrides at
# apply time. Kept in lockstep with the literal in deployment.yaml.
IMAGE_PLACEHOLDER := REPLACE_ME_ECR_REPO/kinference:latest

# Applied in THIS order — the ServiceAccount and roles first, so the Deployment
# never briefly references RBAC that does not exist yet.
#
# The router's own five are listed explicitly, so a stray .yaml at this level
# cannot be deployed by accident.
#
# Everything under demo-configs/ is globbed instead. Those are the custom
# resources an operator adds and removes routinely — worker sets, pools,
# policies, providers — and requiring a Makefile edit per fleet defeats the
# point of them being CRs. `sort` keeps the order deterministic, and they go
# LAST so the CRs land after the router that will materialize them.
#
# Applying every policy in there is deliberate and safe: a policy is inert until
# some pool names it in `policyRef`, which the STATE column reports as
# `NotReferenced`. Same for the two pool variants — `inference-all` and
# `inference-local` are distinct objects, and a Pick chooses between them by
# name, so both existing is the point rather than a conflict.
#
# One coupling to preserve if these are ever split up again: an
# InferenceProvider and the GatewayBackend its `backendRef` names are a UNIT.
# Applying one without the other produces a dangling reference that kinference
# structurally cannot see — it never reads GatewayBackends — so it surfaces only
# as failed dispatches the caller reports back. Keeping both under one glob is
# what makes a partial apply unrepresentable.
CORE_MANIFESTS := $(addprefix $(MANIFEST_DIR),rbac.yaml configmap.yaml service.yaml deployment.yaml extension.yaml)

# Anchored at the repo root, not at this Makefile, because the CRs do not live
# beside it: they are examples, shared with anyone reading `k8s/examples`, while
# `k8s/kinference` is the router's own deployment. Relative-to-here is what broke the
# last time they moved.
EXAMPLES_DIR   := $(REPO_ROOT)/k8s/examples/demo-configs
DEMO_MANIFESTS := $(sort $(wildcard $(EXAMPLES_DIR)/*.yaml))
MANIFESTS      := $(CORE_MANIFESTS) $(DEMO_MANIFESTS)

# Deliberately NOT in MANIFESTS. The PodMonitor needs the Prometheus Operator
# CRDs, which a Docker Desktop cluster does not have — folding it into `deploy`
# would make a routine redeploy fail on the laptop path. Same reasoning that
# keeps `crds` out of `deploy`: a dependency that outlives the deployment gets
# its own target.
GRAFANA_DIR    := $(REPO_ROOT)/k8s/grafana

# Listed explicitly rather than globbed, for the same reason CORE_MANIFESTS is:
# not everything in that directory is a Kubernetes object. `kubectl apply -f
# <dir>` reads .json as well as .yaml, so a glob picks up kinference-dashboard.json
# — a Grafana dashboard, not a manifest — and dies on "apiVersion not set,
# kind not set".
GRAFANA_MANIFESTS := $(addprefix $(GRAFANA_DIR)/,kinference-podmonitor.yaml grafana-dashboard-configmap.yaml)

# A glob that matches nothing is silent, and that is how moving these quietly
# reduced `deploy` to the router alone — no error, just four files instead of
# sixteen. The two causes need different words: a missing directory means the
# path in THIS file is stale, an empty one means there is nothing to apply.
ifeq ($(wildcard $(EXAMPLES_DIR)/.),)
$(error $(EXAMPLES_DIR) does not exist. The CR examples moved; fix EXAMPLES_DIR \
in $(lastword $(MAKEFILE_LIST)) rather than deploying the router alone.)
else ifeq ($(strip $(DEMO_MANIFESTS)),)
$(warning No CRs found: $(EXAMPLES_DIR)/*.yaml matched nothing, so `deploy` will \
apply the router ONLY — no worker sets, pools, policies or providers.)
endif

# Concatenate the manifests into one stream. The `---` matters: these files do
# not all end with a document separator, so a plain `cat` merges the last doc of
# one file into the first of the next.
RENDER := awk 'FNR==1 && NR!=1 {print "---"} {print}' $(MANIFESTS)

.PHONY: help ecr-login dashboard build push deploy undeploy crds local-setup \
        monitoring dashboard-configmap \
        check-tag check-registry check-kubeconfig check-local

help: ## list targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "  TARGET=$(TARGET)  image=$(IMAGE)  platform=$(PLATFORM)"
	@echo ""
	@echo "  AWS:            make build push deploy TAG=\$$(git rev-parse --short HEAD)"
	@echo "  Docker Desktop: make local-setup TARGET=local"
	@echo "                  make build push deploy TARGET=local TAG=dev"

ecr-login: check-registry ## docker login to ECR (needs `aws` CLI configured)
	@echo "▶  docker login → $(ECR_REGISTRY)"
	aws ecr get-login-password --region $(AWS_REGION) \
	  | docker login --username AWS --password-stdin $(ECR_REGISTRY)

build: check-registry check-tag dashboard ## build SPA + image, host arch for TARGET=local (TAG=...)
	@echo "▶  docker build $(IMAGE):$(TAG)"
	docker build --platform=$(PLATFORM) \
	  --build-arg KINFERENCE_VERSION=$(TAG) \
	  -t $(IMAGE):$(TAG) \
	  $(REPO_ROOT)

push: check-registry check-tag ## docker push to ECR (TAG=...; no-op for TARGET=local)
ifeq ($(TARGET),local)
	@echo "▶  push: nothing to do — Docker Desktop's Kubernetes reads the local image store"
	@# Fail loudly if the image is somehow absent, rather than letting `deploy`
	@# discover it as an ImagePullBackOff two minutes later.
	@docker image inspect $(IMAGE):$(TAG) >/dev/null 2>&1 || { \
	  echo "ERROR: $(IMAGE):$(TAG) is not in the local image store."; \
	  echo "       Run: make build TARGET=local TAG=$(TAG)"; \
	  exit 1; \
	}
	@echo "✓  $(IMAGE):$(TAG) present locally"
else
	@echo "▶  docker push $(IMAGE):$(TAG)"
	docker push $(IMAGE):$(TAG)
endif

deploy: check-registry check-tag check-kubeconfig ## apply manifests with image pinned to TAG
	@echo "▶  Rendering manifests with image $(IMAGE):$(TAG)"
	@# Substitute the placeholder image with the chosen tag between render
	@# and apply, so deployment.yaml stays untouched on disk — no risk of
	@# accidentally committing a tag pinning.
	@$(RENDER) \
	  | sed "s|$(IMAGE_PLACEHOLDER)|$(IMAGE):$(TAG)|g" \
	  | $(KUBECTL) apply -f -
	@echo "▶  Waiting for rollout to complete..."
	@$(KUBECTL) rollout status deployment/kinference -n default --timeout=2m
	@echo "✓  kinference at $(IMAGE):$(TAG) is live"
	@echo ""
	@echo "  Reach it with:"
	@echo "    $(KUBECTL) -n default port-forward svc/kinference 50051:50051 9001:9001"

undeploy: check-kubeconfig ## remove the kinference Deployment, Service, ConfigMap, RBAC, Extension
	@echo "▶  Deleting kinference resources..."
	@$(RENDER) | $(KUBECTL) delete --ignore-not-found -f -
	@echo "✓  kinference removed"

dashboard: ## build the dashboard SPA (npm ci on first run, npm run build always)
	@if [ ! -d $(DASHBOARD_DIR)/node_modules ]; then \
	  echo "▶  npm ci (first run)"; \
	  cd $(DASHBOARD_DIR) && npm ci; \
	fi
	@echo "▶  npm run build"
	@cd $(DASHBOARD_DIR) && npm run build

crds: check-kubeconfig ## install/update the CRDs (both targets; not part of deploy)
	@# Cluster-scoped and shared, so they outlive any one deployment — which is
	@# why `deploy` deliberately does not touch them. Honors $(KUBECTL), so this
	@# lands on whichever cluster TARGET selects.
	@echo "▶  Applying CRDs to $(CLUSTER_LABEL)"
	@$(KUBECTL) apply -f $(REPO_ROOT)/k8s/crds/
	@echo "✓  CRDs applied"

local-setup: check-local crds ## Docker Desktop prep: CRDs + node label
	@# deployment.yaml pins `nodeSelector: role=system` so kinference never competes
	@# for GPU-tainted nodes. A laptop has one unlabelled node, so without this
	@# the pod sits Pending with a FailedScheduling event and nothing else wrong.
	@# Labelling is better than branching the manifest: the local cluster then
	@# matches the real one instead of drifting as a special case.
	@#
	@# Guarded on TARGET=local because `--all` is indiscriminate: run against a
	@# real cluster it would label the GPU nodes the nodeSelector exists to avoid.
	@echo "▶  Labelling nodes role=system to satisfy the Deployment's nodeSelector"
	@$(KUBECTL) label nodes --all role=system --overwrite
	@echo "✓  ready for: make build push deploy TARGET=local TAG=dev"

monitoring: check-kubeconfig ## apply the PodMonitor + Grafana dashboard (needs kube-prometheus-stack)
	@# Separate from `deploy` because it depends on the Prometheus Operator CRDs
	@# rather than on kinference — the same reason `crds` is separate. Nothing else
	@# scrapes kinference's /metrics, so until this runs the dashboard renders empty
	@# and the only way to see the metrics is a port-forward to :9001.
	@echo "▶  Applying PodMonitor + dashboard to $(CLUSTER_LABEL)"
	@$(KUBECTL) apply $(addprefix -f ,$(GRAFANA_MANIFESTS))
	@echo "✓  applied — the Grafana sidecar picks the dashboard up within ~30s"

dashboard-configmap: ## regenerate grafana-dashboard-configmap.yaml from the dashboard JSON
	@# kinference-dashboard.json is the source of truth; the ConfigMap embeds a copy.
	@# Regenerating rather than hand-editing the embedded copy is what stops the
	@# two drifting into a cluster showing something no one can reproduce.
	@#
	@# Plain `kubectl`, NOT $(KUBECTL): --dry-run=client never talks to a
	@# cluster, but kubectl still refuses to run if the --kubeconfig file is
	@# missing — which is how this target once silently emitted a header-only
	@# file on a machine without the harness kubeconfig. The body is generated
	@# to a temp file first (so a kubectl failure aborts before the output is
	@# touched) and grepped for `data:` as a belt-and-suspenders check.
	@echo "▶  Rebuilding grafana-dashboard-configmap.yaml"
	@kubectl create configmap grafana-dashboard-kinference -n default \
	  --from-file=kinference-dashboard.json=$(GRAFANA_DIR)/kinference-dashboard.json \
	  --dry-run=client -o yaml \
	  | sed -e '/^  creationTimestamp: null$$/d' \
	        -e 's|^  name: grafana-dashboard-kinference$$|  name: grafana-dashboard-kinference\n  labels:\n    app.kubernetes.io/name: kinference\n    grafana_dashboard: "1"|' \
	  > $(GRAFANA_DIR)/.cm.body
	@grep -q '^data:' $(GRAFANA_DIR)/.cm.body \
	  || { echo "✗  generated ConfigMap has no data: key — refusing to write" >&2; \
	       rm -f $(GRAFANA_DIR)/.cm.body; exit 1; }
	@sed -n '1,/^# Applied by/p' $(GRAFANA_DIR)/grafana-dashboard-configmap.yaml \
	  > $(GRAFANA_DIR)/.cm.hdr
	@cat $(GRAFANA_DIR)/.cm.hdr $(GRAFANA_DIR)/.cm.body \
	  > $(GRAFANA_DIR)/grafana-dashboard-configmap.yaml
	@rm -f $(GRAFANA_DIR)/.cm.hdr $(GRAFANA_DIR)/.cm.body
	@echo "✓  regenerated — commit both files together"

# guard rails ----------------------------------------------------------

check-tag:
	@if [ -z "$(TAG)" ]; then \
	  echo "ERROR: TAG=... is required, e.g."; \
	  echo "       make $(MAKECMDGOALS) TAG=v1.2.3"; \
	  echo "       make $(MAKECMDGOALS) TAG=\$$(git rev-parse --short HEAD)"; \
	  exit 1; \
	fi

# Only the AWS path needs a registry configured; `local` has none by design.
check-registry:
ifneq ($(TARGET),local)
	@if echo "$(ECR_REGISTRY)" | grep -q '^REPLACE_ME'; then \
	  echo "ERROR: ECR_REGISTRY in $(lastword $(MAKEFILE_LIST)) is still the placeholder."; \
	  echo "       Edit the file and set ECR_REGISTRY to your account's URL,"; \
	  echo "       e.g. 471112539178.dkr.ecr.us-west-2.amazonaws.com"; \
	  exit 1; \
	fi
endif

# Refuses a target-specific action on the wrong cluster.
check-local:
ifneq ($(TARGET),local)
	@echo "ERROR: $(MAKECMDGOALS) only applies to TARGET=local."; exit 1
endif

check-kubeconfig:
ifeq ($(TARGET),local)
	@# A missing context is the difference between deploying nowhere and
	@# deploying to whatever cluster happens to be current.
	@kubectl config get-contexts $(KUBE_CONTEXT) >/dev/null 2>&1 || { \
	  echo "ERROR: no kubeconfig context named '$(KUBE_CONTEXT)'."; \
	  echo "       Enable Kubernetes in Docker Desktop, or pass"; \
	  echo "       KUBE_CONTEXT=<name> (see: kubectl config get-contexts)."; \
	  exit 1; \
	}
	@# And check it ANSWERS. Docker Desktop hands its API server a fresh random
	@# port on restart while the kubeconfig keeps the old one, so a context that
	@# looks fine resolves to a refused connection. Diagnosing that from
	@# `apply`'s "failed to download openapi" is needlessly hard.
	@$(KUBECTL) get --raw=/version >/dev/null 2>&1 || { \
	  echo "ERROR: context '$(KUBE_CONTEXT)' exists but the API server is unreachable."; \
	  echo "       Is Kubernetes running in Docker Desktop? If it was just"; \
	  echo "       restarted, its API port changed — Docker Desktop rewrites the"; \
	  echo "       kubeconfig when Kubernetes finishes starting."; \
	  exit 1; \
	}
else
	@if [ ! -f "$(KUBECONFIG_PATH)" ]; then \
	  echo "ERROR: kubeconfig not found at $(KUBECONFIG_PATH)"; \
	  echo "       Run 'make kubeconfig' from the model harness (aws-harness/)."; \
	  exit 1; \
	fi
endif
