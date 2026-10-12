terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Tags applied to every AWS resource through `default_tags` on both providers.
# Matches the cluster Terraform in ../terraform/ so a single tag filter finds
# everything (cluster + DNS + CDN).
locals {
  tags = {
    Project = var.project
    Owner   = var.owner
  }
}

# Default provider — S3 bucket lives in us-west-2 alongside the rest of the
# project.  CloudFront is global so it doesn't matter which region it's in.
provider "aws" {
  region = "us-west-2"

  default_tags {
    tags = local.tags
  }
}

# CloudFront requires its ACM certificate to be in us-east-1, so we need a
# second provider aliased to that region — used only for the cert.
provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"

  default_tags {
    tags = local.tags
  }
}
