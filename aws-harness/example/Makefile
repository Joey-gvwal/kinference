# Makefile — Gemma 4 26B MoE cluster lifecycle
#
# Usage:
#   make up              — provision infra + install add-ons (weights stay in S3)
#   make infra           — terraform apply only (no add-on install)
#   make addons          — install/upgrade Helm charts + S3 CSI driver on existing cluster
#   make init-weights    — download Gemma 4 26B MoE AWQ weights to S3 (one-time, ~20-40 min)
#   make down            — terraform destroy (full teardown, $0 ongoing cost)
#   make kubeconfig      — write ~/.kube/wm-inference
#   make status          — show node and pod health
#   make inference       — deploy Gemma 4 26B MoE (reads from S3, ~3-5 min cold-start)
#   make inference-scaled — deploy Gemma 4 26B MoE on both nodes (2 replicas, same S3 PVC)
#   make public          — expose the API via an AWS NLB (public internet)
#   make public ALLOWED_CIDR=x.x.x.x/32  — restrict NLB to a single IP
#   make private         — remove the NLB, revert to internal-only
#   make endpoint        — print the public NLB URL (after make public)
#   make help            — print this message
#
# For the Gemma 4 31B Dense cluster: use Makefile.31b
#   make -f Makefile.31b infra
#   make -f Makefile.31b addons
#   make -f Makefile.31b inference
#
# For cluster-independent ops (DNS, CDN, release uploads, ECR image cache,
# AZ capacity probing): use Makefile.shared
#   make -f Makefile.shared help

CLUSTER_NAME         := wm-inference
AWS_REGION           ?= us-west-2
ALLOWED_CIDR         ?= 0.0.0.0/0
INIT_WEIGHTS_TIMEOUT ?= 7200s
PROJECT              ?= inference
TF_DIR               := terraform
TF_WORKSPACE         := wm-inference

# Each cluster gets its own kubeconfig file so parallel terminal sessions
# never fight over ~/.kube/config or target the wrong cluster.
KUBECONFIG           ?= $(HOME)/.kube/$(CLUSTER_NAME)

export CLUSTER_NAME AWS_REGION KUBECONFIG

.PHONY: up infra upgrade addons init-weights down kubeconfig status gpu-events check-nodes inference inference-scaled inference-down check check-k8s-version public private endpoint test-local test-request linky-server linky-server-down help

## Provision infra and install add-ons in one shot.
## Run one-time setup steps separately (see below).
up: infra addons
	@echo ""
	@echo "Cluster is up. One-time setup (run from your laptop):"
	@echo "  0. Cache container image to ECR (~10-20 min, once per image version):"
	@echo "       make -f Makefile.shared cache-image"
	@echo "  1. Load model weights into S3 (~30-60 min, once per model):"
	@echo "       make init-weights"
	@echo "  Then deploy:"
	@echo "  2. make inference"
	@echo "  3. make public"

## terraform init + apply (infrastructure only)
# Settings are read from terraform/terraform.tfvars — edit that file to change
# cluster_name, aws_region, gpu_capacity_type, etc. without touching source files.
infra:
	@echo "▶  Running terraform init (idempotent — picks up new modules/providers)..."
	cd $(TF_DIR) && env -u TF_WORKSPACE terraform init
	@echo "▶  Ensuring Terraform workspace is '$(TF_WORKSPACE)'..."
	cd $(TF_DIR) && env -u TF_WORKSPACE terraform workspace new $(TF_WORKSPACE) 2>/dev/null; env -u TF_WORKSPACE terraform workspace select $(TF_WORKSPACE)
	@echo "▶  Applying infrastructure (this takes ~15 minutes)..."
	cd $(TF_DIR) && env -u TF_WORKSPACE terraform apply -auto-approve
	@echo "✓  Infrastructure ready"

## Re-initialise Terraform and upgrade all providers and modules to latest allowed versions
upgrade:
	cd $(TF_DIR) && env -u TF_WORKSPACE terraform init -upgrade

## Install / upgrade Kubernetes add-ons on an existing cluster
## Includes: NVIDIA plugin, Gateway API CRDs, S3 CSI driver, agentgateway,
##           llm-d-infra, AWS LBC, GAIE InferencePool, S3 PV/PVC
addons:
	@echo "▶  Installing Kubernetes add-ons..."
	TF_WORKSPACE=$(TF_WORKSPACE) bash scripts/install-addons.sh

## Download Gemma 4 26B MoE AWQ weights from HuggingFace to S3 (idempotent).
## Skips immediately if weights are already in S3 (fast re-run on cluster rebuild).
## Override timeout: make init-weights INIT_WEIGHTS_TIMEOUT=3600s
init-weights: kubeconfig
	@echo "▶  Ensuring model weights are in S3..."
	INIT_WEIGHTS_TIMEOUT="$(INIT_WEIGHTS_TIMEOUT)" bash scripts/init-weights.sh

## Tear down everything — VPC, EKS cluster, node groups (no ongoing cost).
## Model weights remain in S3 and are reused on the next `make up`.
## Runs `make private` first so the NLB is fully deleted before Terraform touches the VPC.
down:
	@echo "▶  Removing public ALB before VPC teardown..."
	@bash scripts/expose.sh private || true
	@echo "▶  Cleaning up stray LBC security groups..."
	@CLUSTER_NAME=$(CLUSTER_NAME) AWS_REGION=$(AWS_REGION) bash scripts/cleanup-lbc-sgs.sh
	@echo "▶  Destroying all infrastructure (this takes ~10-15 minutes)..."
	cd $(TF_DIR) && env -u TF_WORKSPACE terraform workspace select $(TF_WORKSPACE) && env -u TF_WORKSPACE terraform destroy -auto-approve
	@echo "✓  All resources destroyed. No ongoing AWS costs."
	@echo "   Model weights remain in S3 for the next cluster rebuild."

## Write kubeconfig for the 26B cluster to ~/.kube/wm-inference.
kubeconfig:
	aws eks update-kubeconfig --region $(AWS_REGION) --name $(CLUSTER_NAME) --kubeconfig $(KUBECONFIG)
	@echo "✓  kubectl configured for $(CLUSTER_NAME) → $(KUBECONFIG)"

## Watch GPU node provisioning events — run in a second terminal during make infra.
gpu-events:
	@echo "Watching GPU node group events every 10s (Ctrl-C to stop)..."
	@while true; do \
	  echo ""; \
	  echo "── $$(date '+%H:%M:%S') ─────────────────────────────────────────────"; \
	  aws autoscaling describe-scaling-activities \
	    --region $(AWS_REGION) \
	    --query "Activities[?contains(AutoScalingGroupName,'$(CLUSTER_NAME)-gpu')].{Time:StartTime,Status:StatusCode,Reason:StatusMessage}" \
	    --output table 2>/dev/null | head -20; \
	  sleep 10; \
	done

## Show all nodes with instance type and GPU/system role.
check-nodes: kubeconfig
	kubectl get nodes -o custom-columns='NAME:.metadata.name,INSTANCE:.metadata.labels.node\.kubernetes\.io/instance-type,ROLE:.metadata.labels.role'

## Show cluster health at a glance
status: kubeconfig
	@echo ""
	@echo "── Nodes ────────────────────────────────────────"
	kubectl get nodes -L role,nvidia.com/gpu.count
	@echo ""
	@echo "── Gateway / EPP pods ───────────────────────────"
	@kubectl get pods -n llmd --no-headers 2>/dev/null | grep -q . \
	  && kubectl get pods -n llmd \
	  || echo "  (none — gateway not installed yet)"
	@kubectl get pods -n agentgateway-system --no-headers 2>/dev/null | grep -q . \
	  && kubectl get pods -n agentgateway-system \
	  || true
	@kubectl get pods -n default -l 'llm-d.ai/model=gemma-4-26b' --no-headers 2>/dev/null | grep -q . \
	  && kubectl get pods -n default -l 'llm-d.ai/model=gemma-4-26b' \
	  || echo "  (no model pods — run 'make inference')"
	@echo ""
	@echo "── InferencePools ───────────────────────────────"
	@kubectl get inferencepools -A --no-headers 2>/dev/null | grep -q . \
	  && kubectl get inferencepools -A \
	  || echo "  (none deployed yet — run 'make addons')"
	@echo ""
	@echo "── Model deployments ────────────────────────────"
	@kubectl get deployments -n default -l 'helm.sh/chart=llm-d-modelservice-v0.4.9' --no-headers 2>/dev/null | grep -q . \
	  && kubectl get deployments -n default -l 'helm.sh/chart=llm-d-modelservice-v0.4.9' \
	  || echo "  (none — run 'make inference')"
	@echo ""
	@echo "── S3 weight store ──────────────────────────────"
	@aws s3 ls s3://471112539178-us-west-2/weights/gemma-4-26b-awq/ --recursive 2>/dev/null \
	  | wc -l | xargs -I{} echo "  {} files in s3://471112539178-us-west-2/weights/gemma-4-26b-awq/"

## Deploy Gemma 4 26B MoE AWQ from S3 (~3-5 min cold-start).
## Requires: make init-weights (weights must be in S3 first).
inference: init-weights
	helm repo update llm-d-modelservice
	helm upgrade --install gemma-4-26b llm-d-modelservice/llm-d-modelservice \
	  --version v0.4.9 \
	  --namespace default \
	  -f k8s/addons/modelservice-values.yaml \
	  --wait --timeout 15m
	@echo "✓  Gemma 4 26B MoE deployed. Run 'make public' to expose publicly."
	@echo "   Watch pod startup: kubectl get pods -n default -w"

## Deploy Gemma 4 26B MoE with 2 replicas across both GPU nodes.
## Both pods share the same S3-backed PVC (ReadOnlyMany) — no volume cloning needed.
## Requires: make init-weights (weights must be in S3 first).
inference-scaled: init-weights
	helm repo update llm-d-modelservice
	helm upgrade --install gemma-4-26b llm-d-modelservice/llm-d-modelservice \
	  --version v0.4.9 \
	  --namespace default \
	  -f k8s/addons/modelservice-values.yaml \
	  --set decode.replicas=2 \
	  --wait --timeout 15m
	@echo "✓  Scaled deployment complete (2 pods across both nodes)."
	@echo "   The InferencePool routes across both pods automatically."
	@echo "   Watch pods: kubectl get pods -n default -w"
	@echo "   Then run:   make public"

## Remove model deployment (helm release only — does not touch S3 weights)
inference-down: kubeconfig
	helm delete gemma-4-26b --namespace default 2>/dev/null || true
	@echo "✓  Model release removed. Weights remain in S3."

## Check whether any tagged resources are actually billing (safe, read-only)
check:
	AWS_REGION=$(AWS_REGION) PROJECT=$(PROJECT) CLUSTER_NAME=$(CLUSTER_NAME) bash scripts/check-spend.sh

## Expose the inference API publicly via an AWS NLB
## Optionally restrict by IP: make public ALLOWED_CIDR=x.x.x.x/32
public: kubeconfig
	bash scripts/expose.sh public "$(ALLOWED_CIDR)"

## Remove the public NLB — revert to internal access only
private: kubeconfig
	bash scripts/expose.sh private

## Print the public NLB hostname + test command (waits until NLB is ready)
endpoint: kubeconfig
	bash scripts/expose.sh endpoint

## Port-forward to the running model pod and send a test request
test-local: kubeconfig
	@echo "▶  Port-forwarding to decode pod on localhost:8000 (Ctrl-C to stop)..."
	@echo "   Test with: make test-request (in a separate terminal)"
	kubectl port-forward -n default deploy/gemma-4-26b-llm-d-modelservice-decode 8000:8000

## Send a test chat completion request to localhost:8000
test-request:
	curl -s http://localhost:8000/v1/chat/completions \
	  -H "Content-Type: application/json" \
	  -d '{"model":"cyankiwi/gemma-4-26B-A4B-it-AWQ-4bit","messages":[{"role":"user","content":"Hello! Reply in one sentence."}],"max_tokens":50}' \
	  | python3 -m json.tool

## Guard: linky-server requires Kubernetes >= 1.29 (native sidecars GA).
## The transcript-uploader sidecar relies on `restartPolicy: Always` on an
## initContainer, which went GA in 1.29. Terraform pins cluster_version to
## 1.31 by default, so this should only ever fail on a hand-rolled cluster.
check-k8s-version: kubeconfig
	@VER=$$(kubectl version -o json 2>/dev/null | python3 -c "import sys,json,re; v=json.load(sys.stdin)['serverVersion']; print(int(re.sub(r'[^0-9]','',v['major'])), int(re.sub(r'[^0-9]','',v['minor'])))"); \
	MAJOR=$${VER%% *}; MINOR=$${VER##* }; \
	if [ -z "$$MAJOR" ] || [ -z "$$MINOR" ]; then \
	  echo "ERROR: could not read server version from kubectl"; exit 1; \
	fi; \
	if [ "$$MAJOR" -lt 1 ] || { [ "$$MAJOR" -eq 1 ] && [ "$$MINOR" -lt 29 ]; }; then \
	  echo "ERROR: linky-server requires Kubernetes >= 1.29 for native sidecar support (found $$MAJOR.$$MINOR)."; \
	  echo "       Upgrade via terraform/variables.tf:kubernetes_version, then 'make infra'."; \
	  exit 1; \
	fi; \
	echo "✓  Kubernetes $$MAJOR.$$MINOR ≥ 1.29 — native sidecars available"

## Deploy linky server components:
##   • Ollama (CPU-only, mxbai-embed-large embeddings)
##   • linky (RAG chatbot, points at 26B model + Ollama)
linky-server: kubeconfig check-k8s-version
	helm repo add ollama https://helm.otwld.com/ 2>/dev/null || true
	helm repo update ollama
	helm upgrade --install ollama ollama/ollama \
	  --namespace ollama \
	  --create-namespace \
	  -f k8s/linky/ollama-values.yaml \
	  --wait --timeout 10m
	@echo "✓  Ollama deployed (CPU-only, mxbai-embed-large)."
	aws ecr get-login-password --region $(AWS_REGION) | \
	  helm registry login --username AWS --password-stdin \
	  471112539178.dkr.ecr.$(AWS_REGION).amazonaws.com
	kubectl apply -f k8s/linky/vllm-svc-26b.yaml
	@echo "✓  vLLM internal service applied."
	@LINKY_ROLE_ARN=$$(cd $(TF_DIR) && env -u TF_WORKSPACE terraform workspace select $(TF_WORKSPACE) >/dev/null && env -u TF_WORKSPACE terraform output -raw linky_s3_role_arn) && \
	helm upgrade --install linky \
	  oci://471112539178.dkr.ecr.$(AWS_REGION).amazonaws.com/linky-chatbot-server-helm/linky \
	  --version 0.11.0 \
	  --namespace linky \
	  --create-namespace \
	  --set image.repository=471112539178.dkr.ecr.$(AWS_REGION).amazonaws.com/linky-chatbot-server \
	  --set image.tag=v0.11 \
	  --set upstream.url=http://gemma-4-26b-vllm.default.svc.cluster.local:8000 \
	  --set upstream.model=cyankiwi/gemma-4-26B-A4B-it-AWQ-4bit \
	  --set embeddings.url=http://ollama.ollama.svc.cluster.local:11434 \
	  --set embeddings.model=mxbai-embed-large \
	  --set upstream.contextSize=65536 \
	  --set 'search.preferSources=docs.buoyant.io\,changelogs' \
	  --set playbooks.mountPath=/data/playbooks \
	  --set playbooks.existingConfigMap="" \
	  --set transcripts.enabled=true \
	  --set transcripts.uploader.s3.bucket=471112539178-us-west-2 \
	  --set transcripts.uploader.image.tag=2.34.34 \
	  --set serviceAccount.name=linky \
	  --set-string "serviceAccount.annotations.eks\.amazonaws\.com/role-arn=$$LINKY_ROLE_ARN" \
	  --wait --timeout 5m
	@echo "✓  linky deployed."
	kubectl apply -f k8s/linky/httproute-linky.yaml
	@echo "✓  HTTPRoute applied (/linky → linky:8080)."

## Remove linky server deployment.
linky-server-down: kubeconfig
	kubectl delete -f k8s/linky/httproute-linky.yaml 2>/dev/null || true
	kubectl delete -f k8s/linky/vllm-svc-26b.yaml 2>/dev/null || true
	helm delete linky --namespace linky 2>/dev/null || true
	helm delete ollama --namespace ollama 2>/dev/null || true
	@echo "✓  Linky server removed."

## Print available targets
help:
	@grep -E '^##' Makefile | sed 's/^## //'
