# Makefile — EKS harness for KV-event-publishing inference servers
#
# Cluster lifecycle:
#   make up               provision VPC + EKS + system node group (GPU NG starts at 0)
#   make down             tear everything down ($0 ongoing cost after destroy)
#   make kubeconfig       write a cluster-specific kubeconfig to $(KUBECONFIG)
#   make addons           install NVIDIA device plugin + Mountpoint S3 CSI on the cluster
#   make scale-gpu N=1    scale the L40S GPU node group to N nodes (max defined in tfvars)
#   make scale-gpu POOL=h100 N=1    scale the H100 (p5.48xlarge) node group instead
#   make scale-gpu POOL=h200 N=1    scale the H200 (p5e.48xlarge) node group instead
#   make status           quick health snapshot
#   make find-resources   list AWS resources tagged Project=$(PROJECT_TAG) across regions
#
# Weights:
#   make init-weights MODEL=<hf-repo> PREFIX=<s3-rel-prefix>
#                         download HF repo and aws s3 sync into the shared
#                         weights bucket. Idempotent on a sentinel object.
#
# Public exposure (off by default):
#   make install-lbc      install AWS Load Balancer Controller (one-time)
#   make expose-gateway GATEWAY_SVC=<svc>
#                         provision a public NLB for a Gateway API data-plane
#                         Service. GATEWAY_SVC is required — typically
#                         `<gateway-name>-gatewayd` for kgateway/envoy-gateway.
#   make gateway-endpoint GATEWAY_SVC=<svc>
#                         print the NLB URLs (waits if not yet ready)
#   make unexpose-gateway GATEWAY_SVC=<svc>
#                         tear the NLB down
#
# Per-worker exposure for local kinference / score (replaces port-forward):
#   make expose-workers           one NLB exposes all vLLM pods on port-forward.sh's offset scheme
#   make expose-workers-regen     regenerate router config after a pod restart
#   make worker-nlb-host          print the NLB DNS for use with BASE_HOST=…
#   make workers-endpoint         print the full per-pod URL set
#   make unexpose-workers         tear it down
#
# llm-d comparison setup (off by default; fully removable):
#   make install-llm-d SERVER_NAME=qwen-3-6-vllm
#                         deploy llm-d v0.4.0 routing to the named vLLM server
#   make llmd-endpoint    print the llm-d gateway's public NLB URL
#   make uninstall-llm-d  remove llm-d + every dep (also runs from `make down`)
#
# Per-server lifecycle is in Makefile.server:
#   make -f Makefile.server server-up CFG=servers/qwen-vllm.toml
#   make -f Makefile.server port-forward NAME=qwen-vllm
#   make -f Makefile.server kinference-config
#   make -f Makefile.server server-down CFG=servers/qwen-vllm.toml
#
# All AWS resources created here are tagged Project=$(PROJECT_TAG) and
# Owner=$(shell git config user.email).
#
# This Makefile never reads or writes ~/.kube/config; each cluster gets its
# own kubeconfig at $(KUBECONFIG) so parallel sessions don't fight.

CLUSTER_NAME ?= inf-testbed
AWS_REGION   ?= us-west-2
PROJECT_TAG  ?= inference-testbed
TF_DIR       := terraform

# Pulled from git so this harness has no committed personal data. Override
# with `make up OWNER=someone@example.com` if your git config is unset.
OWNER ?= $(shell git config user.email)

# Per-cluster kubeconfig keeps parallel terminals from clobbering each other.
# Deliberately `:=` not `?=` so an inherited KUBECONFIG env var (pointing at
# a previous cluster's config, or the user's default ~/.kube/config) doesn't
# cause writes to the wrong file. To use a different path for this invocation
# only, pass it on the command line: `make kubeconfig KUBECONFIG=/other/path`
# (command-line args always win, even over `:=`).
KUBECONFIG := $(HOME)/.kube/$(CLUSTER_NAME)

# Forward into scripts/.
export CLUSTER_NAME AWS_REGION KUBECONFIG PROJECT_TAG

TF_VARS := -var "cluster_name=$(CLUSTER_NAME)" -var "aws_region=$(AWS_REGION)" -var "owner=$(OWNER)" -var "project=$(PROJECT_TAG)"

# Filter that hides the per-resource diff of a terraform plan but keeps the
# refresh lines, the summary, and any errors. Set VERBOSE=1 to bypass.
ifdef VERBOSE
TF_FILTER := cat
else
TF_FILTER := awk '/Terraform will perform the following actions:/{skip=1; print; print "  ... (resource diff elided; rerun with VERBOSE=1 to see, or read /tmp/tf-*-$(CLUSTER_NAME).log)"; next} /^Plan:/{skip=0} /^Error/{skip=0} !skip'
endif

.PHONY: up down infra addons kubeconfig scale-gpu status find-resources \
        init-weights install-lbc expose-gateway unexpose-gateway gateway-endpoint \
        expose-workers unexpose-workers expose-workers-regen worker-nlb-host \
        workers-endpoint \
        install-llm-d uninstall-llm-d llmd-endpoint \
        install-monitoring uninstall-monitoring expose-grafana unexpose-grafana grafana-endpoint \
        check-owner tf-init tf-fmt tf-validate help

## Provision the cluster (no addons, no GPU nodes).
up: check-owner infra
	@echo ""
	@echo "✓  Cluster '$(CLUSTER_NAME)' is up."
	@echo "   Next:"
	@echo "     make kubeconfig         — write ~/.kube/$(CLUSTER_NAME)"
	@echo "     make addons             — NVIDIA device plugin + Mountpoint S3 CSI"
	@echo "     make scale-gpu N=1      — add a GPU node when you need it"
	@echo "     make -f Makefile.server server-up CFG=servers/<name>.toml"

## terraform init + plan + apply. Suppresses the per-resource diff (which is
## ~hundreds of lines for the EKS module) and just shows the summary line;
## the full plan is always tee'd to /tmp/tf-plan-$(CLUSTER_NAME).log.
## Set VERBOSE=1 to bypass the filter and see everything.
infra: check-owner tf-init
	@echo "▶  terraform plan ($(CLUSTER_NAME) in $(AWS_REGION))..."
	@cd $(TF_DIR) && terraform plan $(TF_VARS) -out=tfplan -compact-warnings 2>&1 \
	  | tee /tmp/tf-plan-$(CLUSTER_NAME).log \
	  | $(TF_FILTER)
	@echo "▶  terraform apply — typically ~12-15 min on first run..."
	cd $(TF_DIR) && terraform apply tfplan
	@rm -f $(TF_DIR)/tfplan

## Initialise Terraform (idempotent — picks up new providers/modules).
tf-init:
	cd $(TF_DIR) && terraform init

## Re-init and upgrade providers/modules to latest allowed versions.
tf-upgrade:
	cd $(TF_DIR) && terraform init -upgrade

tf-fmt:
	cd $(TF_DIR) && terraform fmt

tf-validate: tf-init
	cd $(TF_DIR) && terraform validate

## Refuse to apply if OWNER could not be resolved — keeps every resource tagged.
check-owner:
	@if [ -z "$(OWNER)" ]; then \
	  echo "ERROR: OWNER is empty. Set 'git config user.email' or run with OWNER=you@example.com"; \
	  exit 1; \
	fi
	@echo "▶  Tagging resources Project=$(PROJECT_TAG) Owner=$(OWNER)"

## Install Kubernetes addons (NVIDIA device plugin, Mountpoint S3 CSI).
addons: kubeconfig
	bash scripts/install-addons.sh

## Write a cluster-specific kubeconfig.
kubeconfig:
	aws eks update-kubeconfig --region $(AWS_REGION) --name $(CLUSTER_NAME) --kubeconfig $(KUBECONFIG)
	@echo "✓  kubectl configured: KUBECONFIG=$(KUBECONFIG)"

## Scale a GPU managed node group. Defaults POOL=gpu (L40S), N=1.
##   make scale-gpu                       # L40S: scales to 1
##   make scale-gpu N=2                   # L40S: scales to 2
##   make scale-gpu N=0                   # L40S: scales to 0 (kills GPU bill)
##   make scale-gpu POOL=h100 N=1         # H100 (p5.48xlarge):  scales to 1
##   make scale-gpu POOL=h200 N=1         # H200 (p5e.48xlarge): scales to 1
##   make scale-gpu POOL=h100 N=0         # H100: scales to 0
##
## POOL matches the terraform node-group key ("gpu" for L40S, "h100" for p5,
## "h200" for p5e). EKS managed node groups use a name *prefix* by default —
## the real name has a timestamp suffix. We resolve it at runtime via
## `list-nodegroups`.
##
## The max-size ceiling defaults per pool to match terraform's variable
## defaults (gpu_max_size=4, h100_max_size=1, h200_max_size=1); scaling beyond
## those triggers drift from terraform state on the next apply. If you need
## durable headroom, bump the tfvar instead.
POOL ?= gpu
N ?= 1
scale-gpu:
	@case "$(POOL)" in \
	  gpu)  MAX_DEFAULT=4 ;; \
	  h100) MAX_DEFAULT=1 ;; \
	  h200) MAX_DEFAULT=1 ;; \
	  *)    MAX_DEFAULT=$(N) ;; \
	esac; \
	NG=$$(aws eks list-nodegroups --region $(AWS_REGION) --cluster-name $(CLUSTER_NAME) --query "nodegroups[?starts_with(@, '$(CLUSTER_NAME)-$(POOL)')] | [0]" --output text); \
	if [ -z "$$NG" ] || [ "$$NG" = "None" ]; then \
	  echo "ERROR: no '$(POOL)' node group found for cluster $(CLUSTER_NAME). Known pools: gpu (L40S), h100 (p5), h200 (p5e). Run 'make infra' first if the cluster is new."; \
	  exit 1; \
	fi; \
	echo "▶  Scaling $(POOL) node group $$NG to $(N)..."; \
	MAX=$$( [ $(N) -lt $$MAX_DEFAULT ] && echo $$MAX_DEFAULT || echo $(N) ); \
	aws eks update-nodegroup-config \
	  --region $(AWS_REGION) \
	  --cluster-name $(CLUSTER_NAME) \
	  --nodegroup-name "$$NG" \
	  --scaling-config minSize=0,desiredSize=$(N),maxSize=$$MAX \
	  >/dev/null
	@echo "✓  Requested desired_size=$(N). Watch with: kubectl get nodes -l gpu-pool=$(POOL) -w"

## Quick health snapshot.
status: kubeconfig
	@echo "── Nodes ────────────────────────────────────────"
	@kubectl get nodes -L role,nvidia.com/gpu.present
	@echo ""
	@echo "── kube-system ──────────────────────────────────"
	@kubectl get pods -n kube-system | grep -E 'nvidia|s3-csi|aws-load' || echo "  (none of NVIDIA / S3 CSI / LBC present yet — run 'make addons')"
	@echo ""
	@echo "── Server instances ─────────────────────────────"
	@kubectl get pods -l mm/managed-by=model-harness --all-namespaces 2>/dev/null | grep -v '^NAMESPACE' || echo "  (no server instances deployed)"

## List AWS resources tagged Project=$(PROJECT_TAG). Use this after 'make down'
## to verify clean teardown.
find-resources:
	bash scripts/find-resources.sh

## Populate the shared weights bucket from a HuggingFace repo, idempotently.
## Runs on your laptop (huggingface_hub.snapshot_download + aws s3 sync); the
## shared bucket is the same one GPU node IAM policy already grants read on.
##
##   make init-weights MODEL=cyankiwi/Qwen3.6-27B-AWQ-INT4 \
##                     PREFIX=weights/qwen-3-6-27b-awq
##
## Re-runs against an already-populated prefix are no-ops via a sentinel
## object. Set HF_TOKEN=... for gated repos.
## `MODE=cluster` (default) submits a Kubernetes Job inside the cluster —
## best for big models / slow connections. `MODE=local` runs on your
## laptop. `SCRATCH_GIB=N` overrides the cluster Job's ephemeral PVC size
## (default 100). See README "Populating the weights bucket".
MODE        ?= cluster
SCRATCH_GIB ?= 100
init-weights:
	@if [ -z "$(MODEL)" ] || [ -z "$(PREFIX)" ]; then \
	  echo "ERROR: pass MODEL=<hf-repo> PREFIX=<s3-relative-prefix>"; \
	  echo "       e.g. make init-weights MODEL=cyankiwi/Qwen3.6-27B-AWQ-INT4 PREFIX=weights/qwen-3-6-27b-awq"; \
	  exit 1; \
	fi
	MODE="$(MODE)" SCRATCH_GIB="$(SCRATCH_GIB)" bash scripts/init-weights.sh "$(MODEL)" "$(PREFIX)"

## Install the AWS Load Balancer Controller (one-time, idempotent).
## Required before `make expose-gateway` — LBC watches Service-of-type-
## LoadBalancer and Ingress resources and provisions the corresponding
## NLBs/ALBs in AWS.
##
## Kept off the default `make addons` path because most harness usage
## doesn't need public exposure (port-forward suffices). Re-runs are
## no-ops once the controller is at the latest chart version.
install-lbc: kubeconfig
	bash scripts/install-lbc.sh

## Expose a Gateway API data-plane Service to the public internet via
## an NLB. Whatever ports the source Service declares are mirrored as
## plain TCP; NLB is L4 pass-through (no TLS termination, no protocol
## awareness — whatever the pod serves is what clients get).
##
## GATEWAY_SVC is REQUIRED. Different clusters name their data-plane
## Services differently, and there is no safe default — pointing at the
## wrong Service silently exposes an internal control plane. The
## Gateway API controller (kgateway, envoy-gateway, etc.) provisions
## a Service per Gateway CR you apply; convention is `<gateway>-gatewayd`.
##
## Prereqs:
##   1. `make install-lbc` — LBC must be running before this is reconciled.
##   2. A Gateway CR exists AND the controller has provisioned the
##      corresponding data-plane Service.
##
## Example:
##   make expose-gateway GATEWAY_SVC=bllmp-gateway-gatewayd
expose-gateway: kubeconfig
	@if [ -z "$(GATEWAY_SVC)" ]; then \
	  echo "ERROR: GATEWAY_SVC is required."; \
	  echo "  Example: make expose-gateway GATEWAY_SVC=<gateway-name>-gatewayd"; \
	  exit 1; \
	fi
	GATEWAY_SVC="$(GATEWAY_SVC)" bash scripts/expose-gateway.sh up

## Remove the public NLB. Polls AWS until the underlying NLB is fully
## gone, so a subsequent `make down` doesn't trip on a still-present LB
## during VPC teardown. GATEWAY_SVC must match what was passed to
## `make expose-gateway`.
##
## Example:
##   make unexpose-gateway GATEWAY_SVC=bllmp-gateway-gatewayd
unexpose-gateway: kubeconfig
	@if [ -z "$(GATEWAY_SVC)" ]; then \
	  echo "ERROR: GATEWAY_SVC is required (the same value passed to expose-gateway)."; \
	  exit 1; \
	fi
	GATEWAY_SVC="$(GATEWAY_SVC)" bash scripts/expose-gateway.sh down

## Print the public URL(s) for the gateway. Waits until the NLB has a
## hostname (the Service initially shows `<pending>` for ~60-90s). NLB
## is L4 so scheme is unknown — output offers http/https/h2c variants.
##
## Example:
##   make gateway-endpoint GATEWAY_SVC=bllmp-gateway-gatewayd
gateway-endpoint: kubeconfig
	@if [ -z "$(GATEWAY_SVC)" ]; then \
	  echo "ERROR: GATEWAY_SVC is required."; \
	  echo "  Example: make gateway-endpoint GATEWAY_SVC=<gateway-name>-gatewayd"; \
	  exit 1; \
	fi
	GATEWAY_SVC="$(GATEWAY_SVC)" bash scripts/expose-gateway.sh endpoint

## Expose every vLLM/SGLang worker pod through ONE public NLB, using
## the same offset-port scheme as scripts/port-forward.sh:
##   pod 0 → :8000 (http) / :5557 (pub) / :5558 (rep)
##   pod 1 → :9000        / :6557      / :6558
##   ...up to 8 pods.
##
## Use case: run kinference (or any per-pod ZMQ consumer) locally on
## a laptop without `kubectl port-forward`'s flakiness. The NLB
## hostname is stable across worker restarts; the offset port map is
## stable across vLLM scale events (static 8-slot listener set).
##
## After a vLLM pod restarts (new pod IP), re-run `make expose-workers-regen`
## to regenerate the router config — the NLB itself stays put.
expose-workers: kubeconfig
	bash scripts/expose-workers.sh up

## Regenerate the worker-router's nginx config from the CURRENT vLLM
## pod IPs and roll its pod. No NLB churn (~10s round-trip). Run this
## any time a worker pod has restarted or you've scaled the GPU pool.
expose-workers-regen: kubeconfig
	bash scripts/expose-workers.sh regen

## Tear down worker exposure. NLB is removed and we poll AWS until
## it's fully gone (so a subsequent `make down` doesn't trip on a
## still-present LB). Then the router Deployment + ConfigMap.
unexpose-workers: kubeconfig
	bash scripts/expose-workers.sh down

## Print the worker-router NLB hostname (and ONLY that) on stdout.
## Designed to be safe in command substitution:
##   BASE_HOST=$(make worker-nlb-host) bash scripts/gen-kinference-config.sh
##
## No `kubeconfig` prereq on purpose — that target writes to stdout
## (the aws-eks-update + "✓ kubectl configured" lines), which would
## leak into command substitution. Caller must have already run
## `make kubeconfig` once for this cluster (the file is sticky).
worker-nlb-host:
	@kubectl -n default get svc worker-router-public \
	  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}{"\n"}' 2>/dev/null \
	  || echo ""

## Same as worker-nlb-host but prints all per-pod URLs + the
## BASE_HOST one-liner for gen-kinference-config.
workers-endpoint: kubeconfig
	bash scripts/expose-workers.sh endpoint

## Install llm-d v0.4.0 (chart v1.4.0) with prefix-aware scoring,
## routing to the harness's vLLM workers. Layered install:
##   Gateway API CRDs → agentgateway → llm-d-infra → GAIE InferencePool
##   → HTTPRoute → public NLB Service.
## Idempotent — re-runs no-op once each layer is at the pinned version.
##
## Prereqs:
##   1. `make install-lbc` — NLB exposure depends on it.
##   2. At least one vLLM server running. Override via SERVER_NAME=…
##      to point the InferencePool at a different mm/server-name.
##
## Default routes to mm/server-name=qwen-3-6-vllm. Override:
##   make install-llm-d SERVER_NAME=my-vllm
install-llm-d: kubeconfig
	SERVER_NAME="$(SERVER_NAME)" bash scripts/install-llm-d.sh

## Tear down everything install-llm-d deployed. Removes the NLB Service,
## HTTPRoute, GAIE InferencePool, llm-d-infra, agentgateway, llm-d-
## specific CRDs, webhooks, and the llmd + agentgateway-system
## namespaces. Idempotent — safe to re-run. Called automatically by
## `make down` via cleanup-lbc-sgs.sh.
uninstall-llm-d: kubeconfig
	bash scripts/uninstall-llm-d.sh

## Print the public URLs of the llm-d gateway NLB. Two listener ports,
## one per agentgateway flavour (default EPP / kinference EPP). Waits
## until the NLB hostname is assigned (~60-90s after `install-llm-d`).
llmd-endpoint: kubeconfig
	@NLB_HOST=""; \
	echo "▶  Waiting for llmd-gateway-public hostname (Ctrl-C safe)..."; \
	until NLB_HOST=$$(kubectl -n llmd get svc llmd-gateway-public \
	    -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null) \
	    && [ -n "$$NLB_HOST" ]; do \
	  printf '.'; sleep 5; \
	done; \
	echo ""; \
	echo ""; \
	echo "  llm-d gateway URLs (same hostname, different ports):"; \
	echo ""; \
	echo "    Default EPP (llm-d GAIE scheduler):"; \
	echo "      http://$$NLB_HOST:8081/v1/chat/completions"; \
	echo ""; \
	echo "    Monitor EPP (kinference ext_proc on :50052):"; \
	echo "      http://$$NLB_HOST:8082/v1/chat/completions"; \
	echo ""; \
	echo "  Test (substitute :8081 ↔ :8082 to switch backends):"; \
	echo "    curl -i http://$$NLB_HOST:8081/v1/chat/completions \\"; \
	echo "      -H 'Content-Type: application/json' \\"; \
	echo "      -d '{\"model\":\"<served-model-name>\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}'"

## Install kube-prometheus-stack (Prometheus + Grafana + Operator + kube-state-
## metrics + node-exporter), plus the harness's PodMonitor that targets every
## inference pod (vLLM + SGLang) and the vendored vLLM Grafana dashboards.
## Lands in the `monitoring` namespace. Idempotent — re-running with the same
## chart version is a no-op. Required before `make expose-grafana`.
install-monitoring: kubeconfig
	bash scripts/install-monitoring.sh

## Tear down the monitoring stack. Polls AWS until the Grafana NLB (if any) is
## fully removed, then helm-uninstalls and deletes the namespace. Idempotent
## and safe to re-run. Called automatically by `make down` via cleanup-lbc-sgs.sh.
uninstall-monitoring: kubeconfig
	bash scripts/uninstall-monitoring.sh

## Expose Grafana to the public internet via an NLB. Requires `make install-lbc`
## first. Login info printed by `make grafana-endpoint` once the NLB is up.
expose-grafana: kubeconfig
	bash scripts/expose-grafana.sh up

## Remove the Grafana NLB; waits for AWS to confirm deletion so subsequent
## destroy commands aren't blocked by a half-deleted load balancer.
unexpose-grafana: kubeconfig
	bash scripts/expose-grafana.sh down

## Print the Grafana URL and admin password. Waits for the NLB hostname to be
## assigned if it hasn't yet (~60-90s after `make expose-grafana`).
grafana-endpoint: kubeconfig
	bash scripts/expose-grafana.sh endpoint

## Tear everything down. Cleans up stray LBC security groups first so the VPC
## destroy doesn't get stuck on orphaned ENIs. Same plan-out + filter pattern
## as `infra`; set VERBOSE=1 to see the full destroy plan.
down: check-owner
	@echo "▶  Tearing down Kubernetes-provisioned load balancers first."
	@echo "   NLBs/ALBs created by K8s Services leave ENIs attached to the"
	@echo "   VPC's public subnets, blocking terraform's subnet+IGW deletion"
	@echo "   for the full 15-min ENI-detach retry loop. Removing them first"
	@echo "   turns a 30-min destroy into a 12-min one. Each un-target is"
	@echo "   safely idempotent — no-op if nothing was installed."
	@# Best-effort: if kubectl can't reach the cluster (e.g., already
	@# partially destroyed), skip this whole block and rely on
	@# cleanup-lbc-sgs.sh + terraform's own retry to unstick things.
	@if kubectl --kubeconfig $(KUBECONFIG) version >/dev/null 2>&1; then \
	  echo "   • unexpose-workers"    && $(MAKE) -s unexpose-workers    2>/dev/null || true; \
	  echo "   • unexpose-grafana"    && $(MAKE) -s unexpose-grafana    2>/dev/null || true; \
	  echo "   • uninstall-llm-d"     && $(MAKE) -s uninstall-llm-d     2>/dev/null || true; \
	  echo "   • uninstall-monitoring" && $(MAKE) -s uninstall-monitoring 2>/dev/null || true; \
	  echo "   • auto-discover any leftover gateway NLBs" && \
	  kubectl --kubeconfig $(KUBECONFIG) get svc -A -l mm/managed-by=model-harness \
	    -o jsonpath='{range .items[?(@.spec.type=="LoadBalancer")]}{.metadata.namespace} {.metadata.name}{"\n"}{end}' 2>/dev/null \
	  | while read ns name; do \
	      [ -z "$$name" ] && continue; \
	      echo "     deleting svc/$$ns/$$name (LBC will tear down its NLB)"; \
	      kubectl --kubeconfig $(KUBECONFIG) -n "$$ns" delete svc "$$name" --ignore-not-found >/dev/null 2>&1 || true; \
	    done; \
	else \
	  echo "   (kubectl can't reach the cluster — skipping K8s cleanup, relying on cleanup-lbc-sgs.sh)"; \
	fi
	@echo "▶  Cleaning up stray LBC security groups (no-op if LBC was never installed)..."
	@bash scripts/cleanup-lbc-sgs.sh || true
	@echo "▶  terraform plan -destroy ($(CLUSTER_NAME))..."
	@cd $(TF_DIR) && terraform plan -destroy $(TF_VARS) -out=tfplan -compact-warnings 2>&1 \
	  | tee /tmp/tf-destroy-$(CLUSTER_NAME).log \
	  | $(TF_FILTER)
	@echo "▶  terraform apply (destroy) — typically ~10-15 min..."
	cd $(TF_DIR) && terraform apply tfplan
	@rm -f $(TF_DIR)/tfplan
	@echo "✓  Cluster destroyed. Verify with: make find-resources"

## Print available targets.
help:
	@grep -E '^##' Makefile | sed 's/^## //'
