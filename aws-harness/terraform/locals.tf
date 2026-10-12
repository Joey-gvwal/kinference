locals {
  # Applied to every AWS resource via provider default_tags.
  tags = {
    Project = var.project
    Owner   = var.owner
  }

  # AZs derived from region so callers never hard-code them.
  azs = [
    "${var.aws_region}a",
    "${var.aws_region}b",
    "${var.aws_region}c",
  ]
}
