module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # Expose the API server publicly so you can run kubectl from your laptop.
  # Lock down cluster_endpoint_public_access_cidrs in production.
  cluster_endpoint_public_access = true

  # Allow the caller's IAM identity to administer the cluster without
  # needing a separate aws-auth ConfigMap entry.
  enable_cluster_creator_admin_permissions = true

  # ── Add-ons managed by EKS ────────────────────────────────────────────────
  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    # EBS CSI driver — needed for PersistentVolumes (model caches, etc.)
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  # ── Node groups ───────────────────────────────────────────────────────────

  eks_managed_node_groups = {

    # ── System nodes (on-demand, always present) ──────────────────────────
    # Runs: KServe controller, cert-manager, llm-d scheduler, cluster services.
    # t3.medium is sufficient and inexpensive (~$0.04/hr each).

    system = {
      name           = "${var.cluster_name}-system"
      instance_types = ["t3.medium"]
      capacity_type  = "ON_DEMAND"

      min_size     = var.system_node_count
      max_size     = var.system_node_count
      desired_size = var.system_node_count

      labels = {
        role = "system"
      }

      iam_role_additional_policies = {
        ecr_read = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
      }
    }

    # ── GPU nodes (for inference) ─────────────────────────────────────────
    # g6e.xlarge:  1x L40S (48 GB VRAM), 4 vCPU,  32 GB RAM  ← primary
    # g6e.2xlarge: 1x L40S (48 GB VRAM), 8 vCPU,  64 GB RAM  ← fallback
    #
    # 48 GB VRAM fits:
    #   • Gemma 4 26B MoE at int8    (~25 GB, 128K ctx)
    #   • Gemma 4 31B Dense at int4  (~16 GB, 256K ctx)
    #   • Gemma 4 E4B at fp16        (~16 GB, 128K ctx)
    #
    # AL2_x86_64_GPU uses Amazon's GPU-optimised AMI with pre-installed
    # NVIDIA drivers — no separate GPU Operator needed.

    gpu = {
      name           = "${var.cluster_name}-gpu"
      instance_types = var.gpu_instance_types
      capacity_type  = var.gpu_capacity_type

      # AL2 GPU AMI: NVIDIA drivers + CUDA pre-installed.
      ami_type = "AL2_x86_64_GPU"

      # Pin all GPU nodes to a single AZ so the EBS model-cache volume
      # (which is AZ-scoped) can be mounted by any inference pod.
      # local.gpu_az defaults to the first AZ in the region.
      subnet_ids = [module.vpc.private_subnets[index(local.azs, local.gpu_az)]]

      min_size     = 0                   # Can scale to 0 without destroy
      max_size     = var.gpu_node_count
      desired_size = var.gpu_node_count

      # disk_size doesn't apply correctly for AL2_x86_64_GPU AMI type.
      # Use explicit block_device_mappings instead.
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.gpu_disk_size_gb
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }

      labels = {
        role                     = "gpu"
        "nvidia.com/gpu.present" = "true"
      }

      # Taint GPU nodes so only GPU-requesting pods are scheduled here.
      taints = [
        {
          key    = "nvidia.com/gpu"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      ]

      # additional policies for S3 bucket access

      iam_role_additional_policies = {
        s3_model_weights = aws_iam_policy.s3_model_weights.arn
        ecr_read         = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
      }
    }
  }

  tags = local.tags
}

resource "aws_iam_policy" "s3_model_weights" {
  name = "${var.cluster_name}-s3-model-weights"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::471112539178-us-west-2",
          "arn:aws:s3:::471112539178-us-west-2/*"
        ]
      }
    ]
  })

  tags = local.tags
}

# ── IRSA for AWS Load Balancer Controller ────────────────────────────────────
# Allows the LBC to manage ALBs, NLBs, and their security groups on our behalf.

module "aws_lbc_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name                              = "${var.cluster_name}-aws-lbc"
  attach_load_balancer_controller_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:aws-load-balancer-controller"]
    }
  }

  tags = local.tags
}

# ── IRSA for EBS CSI driver ───────────────────────────────────────────────────

module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name             = "${var.cluster_name}-ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }

  tags = local.tags
}

