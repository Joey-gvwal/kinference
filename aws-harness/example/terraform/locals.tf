locals {
  # Applied to every AWS resource via provider default_tags.
  # Add more keys here to extend tagging across the entire stack.
  tags = {
    Project = var.project
    Owner   = var.owner
  }

  # Convenience: AZs derived from region so you don't need to hard-code them.
  azs = [
    "${var.aws_region}a",
    "${var.aws_region}b",
    "${var.aws_region}c",
  ]

  # AZ where GPU nodes and the EBS model-cache volume live.
  # Defaults to the first AZ in the region (region + "a") when gpu_az is unset.
  gpu_az = var.gpu_az != "" ? var.gpu_az : "${var.aws_region}a"
}
