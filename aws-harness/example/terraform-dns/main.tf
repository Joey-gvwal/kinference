terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"

  # All resources inherit these tags automatically — matches the cluster
  # Terraform in ../terraform/ so a single tag filter finds everything.
  default_tags {
    tags = {
      Project = var.project
      Owner   = var.owner
    }
  }
}
