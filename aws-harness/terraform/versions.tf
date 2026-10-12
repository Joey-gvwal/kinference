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

  # Default: local state in terraform/terraform.tfstate.
  # For team use or repeated apply/destroy cycles, uncomment + configure:
  #
  # backend "s3" {
  #   bucket = "your-tf-state-bucket"
  #   key    = "model-harness/terraform.tfstate"
  #   region = "us-west-2"
  # }
}
