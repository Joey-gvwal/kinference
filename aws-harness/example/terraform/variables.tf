variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster (used as a prefix on all resources)"
  type        = string
  default     = "inference"
}

variable "kubernetes_version" {
  description = "Kubernetes version for the EKS cluster"
  type        = string
  default     = "1.31"
}

# ── Cost attribution tags ─────────────────────────────────────────────────────

variable "project" {
  description = "Project tag applied to all AWS resources for cost attribution"
  type        = string
  default     = "inference"
}

variable "owner" {
  description = "Owner tag applied to all AWS resources for cost attribution"
  type        = string
  default     = "william@buoyant.io"
}

# ── GPU node group ────────────────────────────────────────────────────────────

variable "gpu_node_count" {
  description = "Number of GPU nodes in the spot node group (set to 0 to scale down cheaply without full destroy)"
  type        = number
  default     = 2
}

variable "gpu_az" {
  description = <<-EOT
    Availability Zone where all GPU nodes and the EBS model-cache volume are placed.
    Must be one of the three AZs in the chosen region (e.g. "us-west-2a").
    Empty string (default) = first AZ in the region (region + "a").

    All GPU nodes are pinned to a single AZ so that the EBS model-cache PVC
    (which is AZ-scoped) can be mounted by any inference pod without cross-AZ issues.
  EOT
  type    = string
  default = ""
}

variable "gpu_instance_types" {
  description = <<-EOT
    Ordered list of instance types for the GPU node group.
    AWS picks the cheapest available type from this list at launch time.
    Providing more types improves fulfillment rates (especially for spot).

    All entries below have a single 48 GB VRAM GPU (NVIDIA L40S, g6e family).
    48 GB fits Gemma 4 26B MoE at int8 (~25 GB) or 31B Dense at int4 (~16 GB)
    with headroom for long-context KV caches (128K–256K tokens).

    g6e.xlarge:  4 vCPU,  32 GB RAM, 1x L40S (48 GB VRAM) ~$2.20/hr on-demand
    g6e.2xlarge: 8 vCPU,  64 GB RAM, 1x L40S (48 GB VRAM) — fallback / larger RAM
  EOT
  type        = list(string)
  default     = ["g6e.xlarge", "g6e.2xlarge"]
}

variable "gpu_capacity_type" {
  description = <<-EOT
    Capacity type for the GPU node group.
    SPOT     — 60-80% cheaper, but requires the "All G and VT Spot Instance
               Requests" Service Quota to be > 0 (default is 0 on new accounts).
    ON_DEMAND — uses your existing 768 vCPU on-demand quota; costs ~3x more
               but works immediately while you wait for spot quota approval.
    Switch back to SPOT once your quota increase is approved.
  EOT
  type        = string
  default     = "SPOT"

  validation {
    condition     = contains(["SPOT", "ON_DEMAND"], var.gpu_capacity_type)
    error_message = "gpu_capacity_type must be SPOT or ON_DEMAND."
  }
}

variable "gpu_disk_size_gb" {
  description = "Root EBS disk size (GB) per GPU node. 100 GB accommodates most model weights."
  type        = number
  default     = 100
}

# ── System node group ─────────────────────────────────────────────────────────

variable "system_node_count" {
  description = "Number of on-demand system nodes (runs KServe, cert-manager, cluster services)"
  type        = number
  default     = 2
}
