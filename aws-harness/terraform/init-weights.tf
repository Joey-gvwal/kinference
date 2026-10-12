# In-cluster `make init-weights` support: IAM role + IRSA + ServiceAccount.
#
# A Kubernetes Job (k8s/jobs/init-weights.yaml.tmpl, submitted by
# scripts/init-weights.sh) runs `huggingface_hub.snapshot_download` →
# `aws s3 sync` inside the cluster, leveraging AWS's S3-region backbone
# instead of the user's home connection. The Job's pod assumes the IAM
# role defined here via IRSA bound to the `default:init-weights` SA.
#
# Permissions are intentionally narrower than the GPU node role's read-
# only policy: only short-lived init-weights pods need write access, not
# long-running inference servers.

resource "aws_iam_policy" "init_weights" {
  name = "${var.cluster_name}-init-weights"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          "arn:aws:s3:::${var.weights_bucket}",
          "arn:aws:s3:::${var.weights_bucket}/*",
        ]
      }
    ]
  })

  tags = local.tags
}

# IRSA role assumable by the `default:init-weights` ServiceAccount.
module "init_weights_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${var.cluster_name}-init-weights"
  role_policy_arns = {
    s3 = aws_iam_policy.init_weights.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["default:init-weights"]
    }
  }

  tags = local.tags
}

# ServiceAccount carrying the eks.amazonaws.com/role-arn annotation that
# tells EKS to inject IRSA-issued credentials when pods using this SA
# call AWS. Terraform-managed so the SA's lifecycle matches the IAM
# role's (e.g. `terraform destroy` removes both).
resource "kubernetes_service_account" "init_weights" {
  metadata {
    name      = "init-weights"
    namespace = "default"
    labels = {
      "mm/managed-by" = "model-harness"
      "mm/task"       = "init-weights"
    }
    annotations = {
      "eks.amazonaws.com/role-arn" = module.init_weights_irsa.iam_role_arn
    }
  }
}

output "init_weights_role_arn" {
  description = "IAM role ARN that the init-weights ServiceAccount assumes via IRSA."
  value       = module.init_weights_irsa.iam_role_arn
}
