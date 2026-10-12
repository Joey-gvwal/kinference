module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  # Additional IAM principals granted access to the cluster. Sourced
  # from var.cluster_collaborators — add or remove entries there,
  # never here. Cluster-creator (the terraform runner) always has
  # admin regardless via enable_cluster_creator_admin_permissions
  # above.
  access_entries = {
    for key, cfg in var.cluster_collaborators : key => {
      principal_arn = cfg.principal_arn
      policy_associations = {
        main = {
          policy_arn = "arn:aws:eks::aws:cluster-access-policy/${cfg.policy}"
          # Terraform's static type checker requires both ternary
          # branches to produce identically-shaped objects, so
          # `namespaces` is included in both. The AWS provider
          # ignores it when type = "cluster".
          access_scope = cfg.access_scope_type == "namespace" ? {
            type       = "namespace"
            namespaces = cfg.namespaces
            } : {
            type       = "cluster"
            namespaces = []
          }
        }
      }
    }
  }

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
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  eks_managed_node_groups = {

    # System nodes: kube-system, S3 CSI controller, optional LBC.
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

    # GPU nodes for inference servers. desired_size=0 so a freshly-applied
    # cluster has no GPU cost; the user bumps the size via
    # `make scale-gpu N=<count>` before deploying server instances.
    gpu = {
      name           = "${var.cluster_name}-gpu"
      instance_types = var.gpu_instance_types
      capacity_type  = "ON_DEMAND"

      # Amazon Linux 2023 NVIDIA-optimised AMI: ships NVIDIA drivers + CUDA,
      # no GPU Operator needed. AL2_x86_64_GPU was retired in EKS 1.33; this
      # is the supported replacement on 1.33+.
      ami_type = "AL2023_x86_64_NVIDIA"

      # Subnet selection for GPU nodes:
      #   var.gpu_az == ""  → multi-AZ (default): ASG can place nodes in
      #     any of the region's private subnets, which is robust to single-
      #     AZ capacity exhaustion. g6e is regularly out in one AZ while
      #     plentiful in others; multi-AZ lets the ASG simply pick whichever
      #     AZ has inventory at launch time.
      #   var.gpu_az != ""  → pin to that AZ. Use when GPU pods need to
      #     stay in the same AZ as AZ-scoped EBS volumes. Not needed today
      #     (weights live in S3 via Mountpoint, which is multi-AZ-safe).
      subnet_ids = var.gpu_az == "" ? module.vpc.private_subnets : [
        module.vpc.private_subnets[index(local.azs, var.gpu_az)]
      ]

      min_size     = 0
      max_size     = var.gpu_max_size
      desired_size = 0

      # disk_size doesn't apply correctly for custom GPU AMIs; use explicit
      # block_device_mappings instead.
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
        # gpu-pool: which GPU class this node belongs to. Pods opt in
        # via nodeSelector on the same key. Separates L40S (this group)
        # from H200 (the p5e node group below) so a small model doesn't
        # accidentally schedule onto a $60/hr H200 node.
        "gpu-pool" = "l40s"
      }

      # Taint GPU nodes so only pods that tolerate nvidia.com/gpu land here.
      taints = [
        {
          key    = "nvidia.com/gpu"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      ]

      iam_role_additional_policies = {
        s3_model_weights = aws_iam_policy.s3_model_weights.arn
        ecr_read         = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
      }
    }

    # H100 nodes for large models whose kernels are SM90-only (e.g.
    # DeepSeek-V4-Flash's fp8_mega_moe MoE kernel doesn't run on Ada).
    # Cheapest tier with H100/H200-only kernel support at ~$55/hr.
    # Default desired_size=0; scale via `make scale-gpu POOL=h100 N=<count>`.
    h100 = {
      name           = "${var.cluster_name}-h100"
      instance_types = var.h100_instance_types
      capacity_type  = "ON_DEMAND"

      ami_type = "AL2023_x86_64_NVIDIA"

      # Pin H100 nodes to a single AZ. Same rationale as h200: p5.48xlarge
      # has narrow AZ availability and EC2 doesn't auto-retry across AZs
      # on "Unsupported" errors. Multi-AZ placement is actively harmful.
      subnet_ids = [
        module.vpc.private_subnets[index(local.azs, var.h100_az)]
      ]

      min_size     = 0
      max_size     = var.h100_max_size
      desired_size = 0

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.h100_disk_size_gb
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }

      labels = {
        role                     = "gpu"
        "nvidia.com/gpu.present" = "true"
        "gpu-pool"               = "h100"
      }

      taints = [
        {
          key    = "nvidia.com/gpu"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      ]

      iam_role_additional_policies = {
        s3_model_weights = aws_iam_policy.s3_model_weights.arn
        ecr_read         = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
      }
    }

    # H200 nodes for very large (~700B+) or hybrid MoE models that need
    # tensor-parallel-size 8 across a single node. Kept separate from the
    # L40S group because per-node cost is ~30× — accidental scheduling
    # here would be expensive. Default desired_size=0; user bumps via
    # `make scale-gpu POOL=h200 N=<count>` when deploying GLM-5.2 etc.
    h200 = {
      name           = "${var.cluster_name}-h200"
      instance_types = var.h200_instance_types
      capacity_type  = "ON_DEMAND"

      ami_type = "AL2023_x86_64_NVIDIA"

      # Pin H200 nodes to a single AZ. Unlike L40S (multi-AZ, capacity
      # fallback across AZs works for InsufficientInstanceCapacity),
      # p5e/p5en have narrow AZ availability — usually only us-west-2c.
      # An ASG that picks an unsupported AZ fails outright with
      # "Unsupported" (EC2 doesn't auto-retry that error class), so
      # multi-AZ placement is actively harmful here.
      subnet_ids = [
        module.vpc.private_subnets[index(local.azs, var.h200_az)]
      ]

      min_size     = 0
      max_size     = var.h200_max_size
      desired_size = 0

      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = var.h200_disk_size_gb
            volume_type           = "gp3"
            delete_on_termination = true
          }
        }
      }

      labels = {
        role                     = "gpu"
        "nvidia.com/gpu.present" = "true"
        "gpu-pool"               = "h200"
      }

      taints = [
        {
          key    = "nvidia.com/gpu"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      ]

      iam_role_additional_policies = {
        s3_model_weights = aws_iam_policy.s3_model_weights.arn
        ecr_read         = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
      }
    }
  }

  tags = local.tags
}

# IAM policy granting GPU nodes read access to the shared weights bucket.
# Mountpoint S3 CSI uses the node instance profile to authenticate.
resource "aws_iam_policy" "s3_model_weights" {
  name = "${var.cluster_name}-s3-model-weights"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.weights_bucket}",
          "arn:aws:s3:::${var.weights_bucket}/*"
        ]
      }
    ]
  })

  tags = local.tags
}

# IRSA for the EBS CSI driver (managed addon expects this role).
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

# IRSA for the AWS Load Balancer Controller. The Helm install only happens
# when the user runs `make expose-server`, but the role itself is cheap to
# pre-create so the install path is simpler.
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
