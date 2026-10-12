variable "aws_region" {
  description = "AWS region to deploy into. Defaults to us-west-2 so the cluster can mount weights from the shared s3://471112539178-us-west-2/weights/ bucket without cross-region transfer."
  type        = string
  default     = "us-west-2"
}

variable "cluster_name" {
  description = <<-EOT
    Name of the EKS cluster (used as a prefix on all resources).
    Keep this <= 15 chars: the terraform-aws-modules/eks module derives IAM
    role name_prefix strings of the form <cluster>-<pool>-eks-node-group-
    which must fit in AWS's 38-char name_prefix limit. With the longest
    pool suffix ("system", 6 chars), the budget is 38 - 23 = 15 chars for
    the cluster name.
  EOT
  type        = string
  default     = "inf-testbed"
}

variable "kubernetes_version" {
  description = "Kubernetes version for the EKS cluster."
  type        = string
  default     = "1.35"
}

# ── Cost attribution tags ─────────────────────────────────────────────────────

variable "project" {
  description = "Project tag applied to all AWS resources for cost attribution."
  type        = string
  default     = "inference-testbed"
}

variable "owner" {
  description = <<-EOT
    Owner tag applied to all AWS resources for cost attribution.
    No default — the Makefile passes `-var owner=$(git config user.email)`
    so the harness has no committed personal data. Override on the CLI
    with `terraform apply -var owner=you@example.com` if invoking directly.
  EOT
  type        = string

  validation {
    condition     = length(var.owner) > 0
    error_message = "owner must be a non-empty string (typically an email address)."
  }
}

# ── Cluster access ────────────────────────────────────────────────────────────

variable "cluster_collaborators" {
  description = <<-EOT
    Additional IAM principals granted access to the cluster, keyed by a
    DNS-safe label used only in terraform state.

    Each entry has:
      principal_arn      IAM user or role ARN.
      policy             One of the AWS-managed cluster-access-policy
                         names:
                           AmazonEKSClusterAdminPolicy — full cluster
                             admin (RBAC edits, CRDs, everything).
                           AmazonEKSAdminPolicy — create/edit any
                             workload, no RBAC or CRD changes. Right
                             choice for "deploy stuff."
                           AmazonEKSEditPolicy — slightly tighter than
                             Admin (no namespace/secret-policy edits).
                           AmazonEKSViewPolicy — read-only.
      access_scope_type  "cluster" for cluster-wide, "namespace" to
                         restrict to specific namespaces (below).
      namespaces         List of namespaces when scope is "namespace";
                         ignored otherwise. Empty for cluster scope.

    Cluster-creator (the terraform runner) always has admin regardless
    of this map — no need to include yourself.

    Add someone by inserting a new entry; remove them by deleting one.
    Nothing else in the harness needs to change.
  EOT
  type = map(object({
    principal_arn     = string
    policy            = string
    access_scope_type = string
    namespaces        = list(string)
  }))
  default = {
    "zahari" = {
      principal_arn     = "arn:aws:iam::471112539178:user/zahari@buoyant.io"
      policy            = "AmazonEKSAdminPolicy"
      access_scope_type = "cluster"
      namespaces        = []
    }
    "alejandro" = {
      principal_arn     = "arn:aws:iam::471112539178:user/alejandro@buoyant.io"
      policy            = "AmazonEKSAdminPolicy"
      access_scope_type = "cluster"
      namespaces        = []
    }
  }
}

# ── GPU node group ────────────────────────────────────────────────────────────

variable "gpu_instance_types" {
  description = <<-EOT
    Ordered list of instance types for the GPU node group. AWS picks the
    first available type at launch time.

    Listing several g6e sizes diversifies the EC2 capacity pools the ASG
    can draw from. g6e.xlarge alone is the most frequently capacity-
    constrained option in us-west-2 (InsufficientInstanceCapacity errors
    in single AZs are common); falling through to .2xlarge / .4xlarge
    lets a scale-up succeed when one pool is dry.

    All entries below have a single 48 GB VRAM GPU (NVIDIA L40S), so any
    server TOML that fits on g6e.xlarge fits on the fallbacks too — only
    per-node CPU/RAM differs.

    g6e.xlarge:   4 vCPU,  32 GB RAM, 1× L40S (48 GB VRAM)  ~$1.86/hr
    g6e.2xlarge:  8 vCPU,  64 GB RAM, 1× L40S (48 GB VRAM)  ~$2.24/hr
    g6e.4xlarge: 16 vCPU, 128 GB RAM, 1× L40S (48 GB VRAM)  ~$3.00/hr

    To add smaller-VRAM fallbacks (e.g. g6.xlarge / g5.xlarge with 24 GB
    L4 / A10G) for broader capacity resilience, append them here — but
    note that any 30B+ FP8 or large-context model in your TOML will OOM
    on the smaller cards. The harness has no per-pod instance-type
    affinity yet, so the scheduler will land pods wherever capacity
    allows.
  EOT
  type        = list(string)
  default     = ["g6e.xlarge", "g6e.2xlarge", "g6e.4xlarge"]
}

variable "gpu_max_size" {
  description = <<-EOT
    Maximum number of GPU nodes in the managed node group.
    The harness starts at desired_size=0 (no GPU cost while idle) and the user
    bumps the count with `make scale-gpu N=<count>` before deploying server
    instances. Default 4 leaves room for several concurrent server instances.
  EOT
  type        = number
  default     = 4
}

variable "gpu_az" {
  description = <<-EOT
    Availability Zone pinning for GPU nodes.

    Empty string (default) = multi-AZ: the ASG can place GPU nodes in
    any of the region's three private subnets. Robust to single-AZ
    capacity exhaustion — g6e is regularly out in one AZ while plentiful
    in others, and AWS will land nodes wherever inventory exists.

    Specific AZ string (e.g. "us-west-2b") = pin all GPU nodes to that
    AZ. The escape hatch for future workloads with AZ-coupled state
    (e.g. EBS volumes attached to GPU pods). The harness today only
    reaches S3 via Mountpoint, which is multi-AZ-safe, so the default
    is fine for current usage.
  EOT
  type        = string
  default     = ""
}

variable "gpu_disk_size_gb" {
  description = "Root EBS disk size (GB) per GPU node. 100 GB accommodates most container images + scratch space; weights live in S3."
  type        = number
  default     = 100
}

# ── H100 GPU node group (p5 for large models with SM90-only kernels) ─────────

variable "h100_instance_types" {
  description = <<-EOT
    Ordered list of instance types for the H100 node group.

    p5.48xlarge: 192 vCPU, 2048 GB RAM, 8× H100 (80 GB HBM3 each = 640 GB total)
                 ~$55/hr on-demand (us-west-2, post-Jun-2025 cut).

    Kept separate from L40S and H200 groups for the same cost-isolation
    reason (see h200 comment). H100 exists as a distinct tier because
    some models — notably DeepSeek-V4-Flash — ship SM90-only kernels
    (fp8_mega_moe) that don't run on Ada Lovelace (L40S = SM89), and
    for those cases p5.48xlarge is the cheapest per-hour option that
    has the required kernel support.
  EOT
  type        = list(string)
  default     = ["p5.48xlarge"]
}

variable "h100_max_size" {
  description = <<-EOT
    Maximum number of H100 nodes in the managed node group. Starts at
    desired_size=0 (no H100 cost while idle); user bumps with
    `make scale-gpu POOL=h100 N=<count>`. Default 1 covers a single
    single-node deployment. Bump the tfvar if you need multiple.
  EOT
  type        = number
  default     = 1
}

variable "h100_disk_size_gb" {
  description = <<-EOT
    Root EBS disk size (GB) per H100 node. 500 GB matches h200 — same
    class of very-large-model deployments, same headroom for container
    image + flashinfer JIT cache. Weights stream from S3, so this isn't
    sized for model files themselves.
  EOT
  type        = number
  default     = 500
}

variable "h100_az" {
  description = <<-EOT
    AZ pinning for H100 (p5.48xlarge) nodes. Same rationale as h200_az:
    p5 has narrow AZ availability and EC2 doesn't auto-retry across AZs
    on "Unsupported" errors, so multi-AZ placement is a footgun.

    Default us-west-2c matches h200_az. Keeping both large-GPU pools in
    the same AZ has a secondary benefit — any cross-node work between
    p5/p5e nodes stays intra-AZ (cheaper egress, lower latency). If
    AWS moves p5 inventory to a different AZ, override with e.g.
    `-var h100_az=us-west-2b`.
  EOT
  type        = string
  default     = "us-west-2c"

  validation {
    condition     = length(var.h100_az) > 0
    error_message = "h100_az must be non-empty. Multi-AZ placement for p5.48xlarge typically doesn't work — AWS's instance-type/AZ pairing is narrow."
  }
}

# ── H200 GPU node group (p5e / p5en for large / hybrid MoE models) ────────────

variable "h200_instance_types" {
  description = <<-EOT
    Ordered list of instance types for the H200 node group. AWS picks the
    first available at launch time.

    p5e.48xlarge:  192 vCPU, 2048 GB RAM, 8× H200 (141 GB VRAM each)   ~$55-60/hr
    p5en.48xlarge: 192 vCPU, 2048 GB RAM, 8× H200 (141 GB VRAM each)   ~$63/hr
                   Same GPUs; adds EFA networking premium — only useful
                   for multi-node training/inference. For single-node
                   serving p5e is sufficient.

    Kept as a separate node group from L40S (var.gpu_instance_types)
    because the pricing gap is enormous (~30× per GPU-hour). A stray
    L40S-sized pod landing on an idle H200 node would waste 7/8 of a
    ~$60/hr instance. The Helm chart pins pods via `gpu-pool` node
    label so scheduling is explicit, not accidental.
  EOT
  type        = list(string)
  default     = ["p5e.48xlarge", "p5en.48xlarge"]
}

variable "h200_max_size" {
  description = <<-EOT
    Maximum number of H200 nodes in the managed node group.
    Starts at desired_size=0 (no H200 cost while idle); the user bumps
    with `make scale-gpu POOL=h200 N=<count>`. Default 1 covers a single
    single-node GLM-5.2 (or similar 700B-class model) deployment. Raise
    only when you truly need multiple 8× H200 nodes — this is $60+/hr
    per node.
  EOT
  type        = number
  default     = 1
}

variable "h200_disk_size_gb" {
  description = <<-EOT
    Root EBS disk size (GB) per H200 node. 500 GB is generous headroom
    for the vLLM container image, flashinfer JIT cache, and any extra
    working files these very-large-model deployments accumulate. Weights
    still stream from S3 via Mountpoint, so this isn't sized for model
    files themselves.
  EOT
  type        = number
  default     = 500
}

variable "h200_az" {
  description = <<-EOT
    AZ pinning for H200 (p5e/p5en) nodes.

    p5e.48xlarge and p5en.48xlarge have limited AZ availability in
    us-west-2 — typically only us-west-2c offers them. A multi-AZ ASG
    that picks us-west-2a or us-west-2b will fail with "Unsupported -
    Your requested instance type is not supported in your requested
    Availability Zone", and EC2 does NOT auto-retry in another AZ for
    that error (unlike InsufficientInstanceCapacity, which does fall
    through).

    Default us-west-2c reflects current AWS inventory. If AWS expands
    p5e to other AZs, or if your account gets capacity reserved in a
    different AZ, override with e.g. `-var h200_az=us-west-2a` — the
    ASG will pin to that AZ's private subnet.
  EOT
  type        = string
  default     = "us-west-2c"

  validation {
    condition     = length(var.h200_az) > 0
    error_message = "h200_az must be non-empty. Multi-AZ placement for p5e/p5en typically doesn't work — AWS's instance-type/AZ pairing is narrow."
  }
}

# ── System node group ─────────────────────────────────────────────────────────

variable "system_node_count" {
  description = "Number of on-demand t3.medium system nodes (run kube-system pods, S3 CSI controller, optional LBC)."
  type        = number
  default     = 2
}

# ── Weights bucket (shared with the prior inference project) ─────────────────

variable "weights_bucket" {
  description = <<-EOT
    Name of the S3 bucket holding model weights.
    Defaults to the shared bucket from the prior inference project. Override
    if you want to maintain your own. The GPU node IAM role gets s3:GetObject
    + s3:ListBucket on this bucket so Mountpoint S3 CSI can read weights.
  EOT
  type        = string
  default     = "471112539178-us-west-2"
}
