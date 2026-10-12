terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }

  # Recommended: use an S3 backend for team use or repeated apply/destroy cycles.
  # Uncomment and configure to enable remote state.
  #
  # backend "s3" {
  #   bucket = "your-tf-state-bucket"
  #   key    = "inference/terraform.tfstate"
  #   region = "us-east-1"
  # }
}
