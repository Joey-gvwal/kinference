# AWS Inference Cluster

EKS-based LLM inference stack using **vLLM + agentgateway + GAIE** on GPU nodes, with KV-cache-aware request routing.

Three cluster variants are supported, each with its own Terraform workspace and Makefile:

| Variant | Makefile | Model | GPU | Footprint |
|---|---|---|---|---|
| **26B MoE AWQ** | `Makefile` | `cyankiwi/gemma-4-26B-A4B-it-AWQ-4bit` | g6e.xlarge (1× L40S) | ~$53/day |
| **31B Dense** | `Makefile.31b` | `google/gemma-4-31B-it` (bf16, TP=4) | g6e.12xlarge (4× L40S) | ~$250/day |
| **31B AWQ** | `Makefile.31b-awq` | `google/gemma-4-31B-it` (AWQ-4bit, single GPU) | g6e.xlarge (1× L40S) | ~$53/day |

Shared / cluster-independent operations (DNS, TLS, release CDN, ECR image cache, AZ capacity probing, AWS resource lookup) live in `Makefile.shared`.

## Architecture

```
Internet
   │
   ▼
AWS ALB [ns: llmd, "inference-public" Ingress that forwards everything to llm-d-inference-gateway]
   │  HTTPS :443 (when ACM cert ISSUED) or HTTP :80
   │
   ▼
llm-d-inference-gateway [ns: llmd, the agentgateway data-plane proxy]
   │  reads HTTPRoutes → InferencePool / Service backendRef
   │  /model/v1/*  → InferencePool (URLRewrite strips /model)
   │  /linky/*     → linky (URLRewrite strips /linky)
   │
   ▼  (for /model requests)
llm-d-inference-scheduler (EPP / endpoint picker) :9002
   │  scores each vLLM pod:
   │    prefix-cache-scorer  ×3.0   ← routes to pod with matching KV prefix
   │    kv-cache-utilization ×2.0   ← prefers pods with more free KV slots
   │    queue-scorer         ×2.0   ← prefers pods with shorter queues
   ▼
vLLM pod  (ns: default, 471112539178.dkr.ecr.us-west-2.amazonaws.com/llm-d-cuda, port 8000)
   │  reports KV-cache occupancy + queue depth back to EPP via gRPC
   ▼
GPU (NVIDIA L40S, 48 GB VRAM)
```

### Namespace breakdown

| Namespace | What lives there | Installed by |
|---|---|---|
| `agentgateway-system` | agentgateway **controller** — watches Gateway API objects and programs the data-plane proxy via xDS | `make addons` |
| `llmd` | agentgateway **data-plane proxy** (`llm-d-inference-gateway` Service + Deployment); also the ALB Ingress when `make public` is run | `make addons` (llm-d-infra chart) |
| `default` | All model workloads: vLLM pods, EPP pod/service, InferencePool CRD, HTTPRoute | `make inference` + `make addons` (GAIE chart) |
| `linky` | Linky RAG chatbot pointed at the local vLLM + Ollama | `make linky-server` |
| `ollama` | CPU-only embeddings service (`mxbai-embed-large`), consumed by Linky | `make linky-server` |

### Key components

- **agentgateway** — Gateway API controller. Watches HTTPRoutes and translates `InferencePool` backendRefs into xDS config for the data-plane proxy. Requires `AGW_ENABLE_INFER_EXT=true` (set by `make addons`).
- **llm-d-infra** — Helm chart that creates the `Gateway` resource and the agentgateway data-plane proxy deployment in `llmd`.
- **GAIE InferencePool** — Gateway API Inference Extension CRD that binds the EPP to the set of vLLM pods matching a label selector.
- **EPP (llm-d-inference-scheduler)** — Endpoint Picker Process. Intercepts each request via gRPC ext-proc, scores all ready pods, and returns the winning pod IP.
- **llm-d-cuda image (custom)** — vLLM with the `llmd_fs_backend` KV connector. Emits `BlockStored` / `BlockRemoved` events over ZeroMQ so the EPP's prefix-cache scorer can route to pods likely to have matching KV blocks. Do **not** substitute `vllm/vllm-openai` — it lacks the KV event hooks.

## Prerequisites

- AWS CLI configured (`aws configure`)
- Terraform ≥ 1.6 (`brew install terraform`)
- kubectl, Helm ≥ 3.12 (`brew install kubectl helm`)
- Python 3 on PATH (used by install scripts and `make -f Makefile.shared dns`)

Gemma 4 is Apache 2.0 — no HF token required. For gated models, see [Gated Models](#gated-models).

## Quickstart (26B MoE AWQ)

```bash
# 0. One-time, per-account: cache the llm-d-cuda image from GHCR to ECR
make -f Makefile.shared cache-image

# 1. Probe AZ capacity before provisioning (optional but recommended)
make -f Makefile.shared find-az

# 2. Provision cluster + install add-ons (~15 min)
make up

# 3. Download weights to S3 (~20-40 min, idempotent — skips if already there)
make init-weights

# 4. Deploy the model (~3-5 min cold-start)
make inference

# 5. Expose publicly via ALB
make public

# 6. Get the endpoint (waits until ALB is ready)
make endpoint
```

For 31B Dense, substitute `make -f Makefile.31b <target>`; for 31B AWQ, `make -f Makefile.31b-awq <target>`. All three Makefiles support the same basic lifecycle targets (`infra`, `addons`, `init-weights`, `inference`, `inference-down`, `public`, `private`, `endpoint`, `status`, `down`).

Tear everything down with `make down` (per-cluster) — $0 ongoing cost; weights remain in S3.

## Make target reference

### `Makefile.shared` — cluster-independent ops

Invoke as `make -f Makefile.shared <target>`. Safe to run without a kubeconfig.

| Target | What it does |
|---|---|
| `dns` | Provision Route 53 zone + ACM cert for `drlinky.buoyant.cloud` (see [DNS & TLS](#dns--tls)) |
| `cdn` | Provision S3 + CloudFront for `releases.drlinky.buoyant.cloud` (see [Releases CDN](#releases-cdn)) |
| `release FILE=./path [KEY=custom/key]` | Upload an artifact to the CDN and invalidate CloudFront |
| `unrelease KEY=… [PURGE=1]` | Soft-delete a release (default, keeps prior versions); `PURGE=1` hard-deletes every version |
| `cache-image` | Mirror the `llm-d-cuda` image from GHCR to ECR (one-time per version) |
| `find-az [GPU_INSTANCE_TYPES=…]` | Probe EC2 capacity for GPU instance types across `us-west-2` AZs |
| `find-aws-resources [PROJECT_TAG=inference]` | List every AWS resource tagged `Project=<tag>` across `us-west-2` + `us-east-1` |
| `help` | Print the target list |

### `Makefile` — 26B MoE AWQ cluster

| Target | What it does |
|---|---|
| `up` | `infra` + `addons` in one shot |
| `infra` | `terraform apply` (workspace `wm-inference`, `terraform/terraform.tfvars`) |
| `addons` | Install/upgrade add-ons (NVIDIA plugin, Gateway API, S3 CSI, agentgateway, llm-d-infra, LBC, GAIE) |
| `init-weights` | Download Gemma 4 26B AWQ weights to S3 (idempotent) |
| `inference` | Helm deploy `gemma-4-26b` (reads weights from S3, ~3-5 min cold-start) |
| `inference-scaled` | Same, but with 2 replicas across both GPU nodes |
| `inference-down` | Uninstall the model Helm release (weights stay in S3) |
| `linky-server` / `linky-server-down` | Deploy/remove Linky chatbot + Ollama (see [Linky](#linky)) |
| `public [ALLOWED_CIDR=x.x.x.x/32]` | Create ALB Ingress + HTTPRoute; auto-wires HTTPS if ACM cert is ISSUED |
| `private` | Delete ALB + HTTPRoute, remove Route 53 ALIAS record |
| `endpoint` | Wait for ALB, upsert Route 53 ALIAS, print public URL |
| `check-nodes` | Show nodes with instance type + role |
| `status` | Nodes, gateway/EPP pods, InferencePools, model deployments, S3 weight store summary |
| `check` | Read-only AWS billing check for tagged resources |
| `test-local` / `test-request` | Port-forward to the vLLM pod and send a test chat completion |
| `down` | `terraform destroy` (removes ALB first; weights remain in S3) |
| `kubeconfig` | Write `~/.kube/wm-inference` |

### `Makefile.31b` — 31B Dense cluster (TP=4, single node)

Invoke as `make -f Makefile.31b <target>`. Same lifecycle as above with model-specific values:

- Workspace `31b`, vars file `terraform/31b.tfvars`
- Single g6e.12xlarge (4× L40S, 4× tensor-parallel vLLM)
- Helm release `gemma-4-31b`, values at `k8s/addons/modelservice-values-31b.yaml`
- 40-minute Helm wait timeout (loading bf16 weights takes a while)
- Kubeconfig written to `~/.kube/wm-inf-31b`

### `Makefile.31b-awq` — 31B AWQ-4bit cluster (single GPU)

Invoke as `make -f Makefile.31b-awq <target>`. Same cost envelope as 26B (~$53/day) but with full 31B-dense quality at 4-bit quantization.

| Target | What it does |
|---|---|
| `infra` | Provision g6e.xlarge (1× L40S, workspace `31b-awq`, vars file `terraform/31b-awq.tfvars`) |
| `addons` | Install add-ons via `scripts/install-addons-31b-awq.sh` |
| `init-weights` | Download `cyankiwi/gemma-4-31B-it-AWQ-4bit` weights to S3 (~20-30 min) |
| `inference` | Helm deploy `gemma-4-31b-awq` (single-GPU, no tensor parallelism) |
| `inference-down` | Uninstall `gemma-4-31b-awq` + its InferencePool |
| `linky-server` / `linky-server-down` | Linky pointed at `gemma-4-31b-awq-vllm` |
| `public` / `private` / `endpoint` | ALB lifecycle (same as other clusters) |
| `status`, `check-nodes`, `gpu-events`, `check` | Diagnostics |
| `down` | `terraform destroy -var-file=31b-awq.tfvars` |
| `kubeconfig` | Write `~/.kube/wm-inf-31b-awq` |

Each cluster's kubeconfig is a separate file under `~/.kube/` so parallel terminal sessions never fight over `~/.kube/config`.

## DNS & TLS

The three clusters share a single public hostname — `drlinky.buoyant.cloud` — provisioned once via `Makefile.shared` and consumed at runtime by `scripts/expose.sh`.

**One-time setup**

```bash
# Provisions Route 53 hosted zone + ACM cert + CAA record
make -f Makefile.shared dns
```

After applying, the output prints four Route 53 nameservers. Add these as an `NS` record for `drlinky` at your `buoyant.cloud` registrar. Once delegation propagates (~5 min), the ACM cert auto-validates via DNS.

Notes:

- **CAA record**: `buoyant.cloud`'s CAA policy does not include Amazon's CA. `terraform-dns/dns.tf` adds a subdomain CAA record permitting `amazon.com`, which takes precedence over the parent.
- **No `aws_acm_certificate_validation` resource**: the cert cannot validate until NS delegation is live, so Terraform would otherwise block indefinitely. AWS validates automatically once delegation propagates.
- **Hands-free integration with `expose.sh`**:
  - `make public` looks up the ACM cert by domain at runtime — if ISSUED, it annotates the Ingress with the cert ARN + HTTPS redirect.
  - `make endpoint` upserts a Route 53 ALIAS `drlinky.buoyant.cloud → ALB` and prints `https://drlinky.buoyant.cloud`.
  - `make private` deletes the ALIAS before tearing down the ALB.
  - All DNS actions are no-ops if the zone/cert don't exist — safe to run without `make -f Makefile.shared dns` first.

Routing on the shared hostname (via HTTPRoutes + URLRewrite):

| Path | Backend |
|---|---|
| `https://drlinky.buoyant.cloud/model/v1/*` | vLLM via InferencePool (inference gateway strips `/model`) |
| `https://drlinky.buoyant.cloud/linky` | Linky chatbot server (gateway strips `/linky`) |

## Releases CDN

Static content (installable binaries, release notes, etc.) is served at `https://releases.drlinky.buoyant.cloud` via S3 + CloudFront with Origin Access Control.

```bash
# One-time: provision the bucket, CloudFront distribution, and ACM cert (us-east-1)
make -f Makefile.shared cdn

# Upload an artifact (auto-invalidates the CloudFront cache)
make -f Makefile.shared release FILE=./dist/linky-cli-v1.0.0
#   → https://releases.drlinky.buoyant.cloud/linky-cli-v1.0.0

# Upload with a custom S3 key
make -f Makefile.shared release FILE=./dist/linky-cli-v1.0.0 KEY=linky-cli/latest

# Soft-delete (adds a delete marker; prior version retained for rollback)
make -f Makefile.shared unrelease KEY=linky-cli-v1.0.0

# Hard-delete (purges every version AND delete markers — unrecoverable)
make -f Makefile.shared unrelease KEY=linky-cli-v1.0.0 PURGE=1
```

Design notes:

- **Separate subdomain** (not a path on `drlinky.buoyant.cloud`) so the ALB / inference gateway stays untouched. CloudFront fronts only S3.
- **CloudFront requires its ACM cert in `us-east-1`** — `terraform-cdn/` uses a second AWS provider aliased to that region for the cert only.
- **Bucket is private** — `BucketOwnerEnforced`, public access blocked. Only CloudFront (via OAC) can read objects.
- **S3 versioning is on** — you can roll back a bad binary without losing the old one.
- **Price class `PriceClass_100`** (US + Europe only) for minimal cost. Bump to `PriceClass_All` in `terraform-cdn/cdn.tf` if you need global POPs.

## Linky Chatbot Server

Linky is a RAG chatbot that queries Buoyant / Linkerd docs. It's deployed next to the inference cluster so it can call the local vLLM directly (no egress cost) and uses Ollama with `mxbai-embed-large` for CPU-only embeddings.

```bash
make linky-server              # 26B — points Linky at gemma-4-26b-vllm
make -f Makefile.31b linky-server     # 31B dense
make -f Makefile.31b-awq linky-server # 31B AWQ

make linky-server-down         # (and the -f variants) to remove
```

Each variant:

1. Installs Ollama (CPU-only) via its Helm chart into `ollama` namespace
2. Applies a `ClusterIP` Service exposing the cluster's vLLM pods (`k8s/linky/vllm-svc*.yaml`)
3. Installs the `linky-chatbot-server` OCI Helm chart from ECR as release `linky` into the `linky` namespace, wired to both
4. Applies `k8s/linky/httproute-linky.yaml` so `/linky` on the main inference gateway routes to the chatbot UI

## Resource Tagging

All Terraform-managed AWS resources across the three workspaces (`terraform/`, `terraform-dns/`, `terraform-cdn/`) are tagged with:

- `Project = inference`
- `Owner = william@buoyant.io`

Applied via `default_tags` on the AWS provider — no need to remember it per-resource. The ALB created by `scripts/expose.sh` is tagged with the same two keys via an `alb.ingress.kubernetes.io/tags` annotation on the Ingress.

**To find every billable resource this repo creates:**

```bash
make -f Makefile.shared find-aws-resources
```

This loops over `us-west-2` (cluster + DNS + S3) and `us-east-1` (CloudFront ACM cert) and lists every ARN tagged `Project=inference`. Override with `PROJECT_TAG=` or `TAG_SEARCH_REGIONS=` for other project names / regions.

**Caveats:**

- Route 53 records (ALIAS, CNAME, CAA) **cannot be tagged** by AWS — only the hosted zone can. The zone is tagged; its records are not.
- CloudFront Origin Access Controls and IAM inline policies are likewise non-taggable.
- To change the owner, edit `variable "owner"` in `terraform/variables.tf`, `terraform-dns/variables.tf`, and `terraform-cdn/variables.tf`, and update the value in `k8s/expose/public-ingress.yaml`.

## Cost

| Resource | Type | Count | $/hr (est.) |
|---|---|---|---|
| EKS control plane | — | 1 | $0.10 |
| System nodes | t3.medium on-demand | 2 | ~$0.08 |
| GPU nodes (26B / 31B-AWQ) | g6e.xlarge on-demand | 1-2 | ~$2.21 each |
| GPU nodes (31B Dense) | g6e.12xlarge on-demand | 1 | ~$10.50 |
| NAT gateway | single-AZ | 1 | $0.045 |
| S3 weight store | standard | — | <$0.01 |
| **Total (26B / 31B-AWQ running)** | | | **~$2.45/hr** |
| **Total (31B Dense running)** | | | **~$10.70/hr** |
| **Total (destroyed)** | | | **$0** |

Shared resources (Route 53 zone, ACM certs, S3 bucket for releases, CloudFront distribution) are negligible (<$1/month combined) and survive `make down`.

GPU nodes default to **on-demand**. To cut costs by 60–80%, switch to spot once your `All G and VT Spot Instance Requests` quota covers g6e:

```hcl
# terraform/terraform.tfvars (or 31b.tfvars / 31b-awq.tfvars)
gpu_capacity_type = "SPOT"
```

New AWS accounts have a default quota of 0 for spot G instances — request an increase first.

## Context Window and Quantization

Gemma 4 26B A4B (default on `Makefile`) is deployed with AWQ-4bit quantization and 128K context window on a single 48 GB L40S. The model uses alternating sliding-window (1024-token) and global attention layers (5:1 ratio) — only 10 of 60 layers need the full-context KV cache, which is what makes 128K feasible on a single GPU.

### VRAM budget on 48 GB L40S

| Model | Precision | VRAM weights | Fits 128K ctx? |
|---|---|---|---|
| Gemma 4 26B MoE (A4B) | AWQ-4bit | ~13 GB | ✅ (current, `Makefile`) |
| Gemma 4 31B Dense | bf16 | ~62 GB | ❌ → need 4× GPU (`Makefile.31b`) |
| Gemma 4 31B Dense | AWQ-4bit | ~16 GB | ✅ (`Makefile.31b-awq`) |
| Llama 3.1 70B | int4 | ~40 GB | ✅ |
| Llama 3.1 70B | int8 | ~70 GB | ❌ → need multi-GPU |

## Gated Models

If you swap in a gated model, create the HF token secret and uncomment `HUGGING_FACE_HUB_TOKEN` in `k8s/model-cache/download-job.yaml`:

```bash
kubectl create secret generic hf-token --from-literal=token=hf_your_token_here
```

## File Structure

```
├── Makefile                   # 26B MoE AWQ cluster
├── Makefile.31b               # 31B Dense cluster (4× L40S, TP=4)
├── Makefile.31b-awq           # 31B AWQ-4bit cluster (single GPU)
├── Makefile.shared            # cluster-independent: dns, cdn, release, cache-image, find-az, find-aws-resources
├── terraform/                 # cluster infrastructure (VPC, EKS, node groups, S3 weights bucket)
├── terraform-dns/             # Route 53 zone + ACM cert for drlinky.buoyant.cloud
├── terraform-cdn/             # S3 + CloudFront for releases.drlinky.buoyant.cloud
├── k8s/
│   ├── addons/                # NVIDIA plugin, GAIE, modelservice values (per cluster)
│   ├── expose/                # ALB Ingress + HTTPRoutes
│   ├── linky/                 # Linky chatbot + Ollama manifests
│   ├── model-cache/           # S3 PV/PVC + weight download jobs
│   └── runtimes/              # vLLM ServingRuntime
└── scripts/
    ├── install-addons*.sh     # addon install per cluster
    ├── init-weights*.sh       # weight download per cluster
    ├── expose.sh              # public/private/endpoint + DNS auto-wiring
    ├── cache-image.sh         # mirror llm-d-cuda GHCR→ECR
    ├── cleanup-lbc-sgs.sh     # delete stray LBC security groups before terraform destroy
    └── check-spend.sh         # read-only billing check
```

## Customisation

- **GPU count**: `gpu_node_count` in the appropriate `*.tfvars`
- **GPU instance type**: `gpu_instance_types` in the `*.tfvars` (broader list improves spot fulfillment)
- **Region**: `aws_region` in `*.tfvars` (also update `AWS_REGION` in the Makefile if overriding from the shell)
- **Remote Terraform state**: uncomment the `backend "s3"` block in `terraform/versions.tf` and create the bucket first
- **Restrict public access by IP**: `make public ALLOWED_CIDR=203.0.113.42/32`

## Troubleshooting

**`backend does not exist` from agentgateway** — `AGW_ENABLE_INFER_EXT=false` by default; `make addons` enables it. If you see this after a manual install, re-run `make addons`.

**HTTPRoute stuck in `BackendNotFound`** — check:
1. `kubectl get deploy agentgateway -n agentgateway-system -o jsonpath='{.spec.template.spec.containers[0].env}'` → confirms `AGW_ENABLE_INFER_EXT=true`
2. `kubectl get clusterrole agentgateway-agentgateway-system -o yaml | grep inference` → confirms RBAC for `inference.networking.k8s.io`
3. `kubectl get inferencepool` → confirms the pool exists

**GPU pods stuck in `Pending`** — if using spot, the selected type may be unavailable in your AZ:
- `make -f Makefile.shared find-az` to probe capacity
- Add more types to `gpu_instance_types` in the `*.tfvars`
- Switch back to `gpu_capacity_type = "ON_DEMAND"` as a fallback

**vLLM OOM at 128K context** — reduce context in `k8s/addons/modelservice-values*.yaml`:
```yaml
args:
  - "--max-model-len=65536"
  - "--gpu-memory-utilization=0.93"
```
If 64K works, tune up. Alternatively, use a more aggressively quantized model variant.

**ACM cert stuck in `PENDING_VALIDATION`** — NS delegation at the registrar must be live before the cert can validate. Check:
```bash
dig drlinky.buoyant.cloud NS +short  # should return 4 Route 53 nameservers
```
If delegation is live but the cert shows `FAILED` with `CAA_ERROR`, verify the CAA record in the zone permits `amazon.com` (this is set up automatically by `terraform-dns/dns.tf`).

**`terraform destroy` fails with `HostedZoneNotEmpty`** — `scripts/expose.sh endpoint` creates an ALIAS record outside Terraform. Run `make private` before `terraform destroy` (the `make down` target does this automatically), or manually delete the ALIAS:
```bash
ZONE_ID=$(cd terraform-dns && terraform output -raw zone_id)
aws route53 list-resource-record-sets --hosted-zone-id "$ZONE_ID"
# locate the ALIAS record and DELETE it via aws route53 change-resource-record-sets
```
