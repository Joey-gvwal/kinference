provider "aws" {
  region = var.aws_region

  # Every resource inherits these tags automatically — no need to repeat them
  # on individual resource blocks.
  default_tags {
    tags = local.tags
  }
}

# Kubernetes and Helm providers are wired to the EKS cluster created below
# via the EKS module. The exec block uses `aws eks get-token` so credentials
# stay in sync with the AWS profile.

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    }
  }
}
