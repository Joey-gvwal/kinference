# cdn.tf — S3 + CloudFront static hosting for releases.drlinky.buoyant.cloud
#
# Run once with `make cdn`.  Depends on terraform-dns (needs the
# drlinky.buoyant.cloud Route 53 zone to exist for DNS validation and the
# ALIAS record).
#
# Upload releases with:
#   aws s3 cp ./my-binary s3://$(terraform output -raw bucket_name)/linky-cli-v1.0.0
# or:
#   make release FILE=./my-binary
#
# Served at https://releases.drlinky.buoyant.cloud/<filename>.

locals {
  subdomain    = "releases.drlinky.buoyant.cloud"
  parent_zone  = "drlinky.buoyant.cloud"
  bucket_name  = "drlinky-releases"
}

# ── Look up the existing Route 53 zone from terraform-dns ────────────────────
data "aws_route53_zone" "drlinky" {
  name         = local.parent_zone
  private_zone = false
}

# ── S3 bucket (private; served only via CloudFront OAC) ──────────────────────
resource "aws_s3_bucket" "releases" {
  bucket = local.bucket_name

  tags = {
    Name = local.bucket_name
  }
}

resource "aws_s3_bucket_public_access_block" "releases" {
  bucket = aws_s3_bucket.releases.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "releases" {
  bucket = aws_s3_bucket.releases.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "releases" {
  bucket = aws_s3_bucket.releases.id

  versioning_configuration {
    status = "Enabled"
  }
}

# ── ACM certificate (us-east-1, required for CloudFront) ─────────────────────
resource "aws_acm_certificate" "releases" {
  provider = aws.us_east_1

  domain_name       = local.subdomain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name = local.subdomain
  }
}

# DNS validation record in the existing drlinky.buoyant.cloud zone.
resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.releases.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = data.aws_route53_zone.drlinky.zone_id
}

# Block until the cert is actually ISSUED — safe here because the zone is
# already live (unlike terraform-dns, where NS delegation happens after apply).
resource "aws_acm_certificate_validation" "releases" {
  provider = aws.us_east_1

  certificate_arn         = aws_acm_certificate.releases.arn
  validation_record_fqdns = [for r in aws_route53_record.cert_validation : r.fqdn]
}

# ── CloudFront Origin Access Control (locks S3 down to CloudFront) ───────────
resource "aws_cloudfront_origin_access_control" "releases" {
  name                              = "${local.bucket_name}-oac"
  description                       = "OAC for ${local.subdomain}"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# ── CloudFront distribution ──────────────────────────────────────────────────
resource "aws_cloudfront_distribution" "releases" {
  enabled             = true
  is_ipv6_enabled     = true
  comment             = local.subdomain
  price_class         = "PriceClass_100" # US + Europe only — cheap
  default_root_object = "index.html"

  aliases = [local.subdomain]

  origin {
    domain_name              = aws_s3_bucket.releases.bucket_regional_domain_name
    origin_id                = "s3-${local.bucket_name}"
    origin_access_control_id = aws_cloudfront_origin_access_control.releases.id
  }

  default_cache_behavior {
    target_origin_id       = "s3-${local.bucket_name}"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    # AWS-managed CachingOptimized policy.
    cache_policy_id = "658327ea-f89d-4fab-a63d-7e88639e58f6"
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.releases.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  tags = {
    Name = local.subdomain
  }
}

# ── S3 bucket policy: only CloudFront (via OAC) may read ─────────────────────
data "aws_iam_policy_document" "bucket_policy" {
  statement {
    sid    = "AllowCloudFrontRead"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.releases.arn}/*"]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.releases.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "releases" {
  bucket = aws_s3_bucket.releases.id
  policy = data.aws_iam_policy_document.bucket_policy.json
}

# ── Route 53 ALIAS: releases.drlinky.buoyant.cloud → CloudFront ──────────────
resource "aws_route53_record" "releases_a" {
  zone_id = data.aws_route53_zone.drlinky.zone_id
  name    = local.subdomain
  type    = "A"

  alias {
    name                   = aws_cloudfront_distribution.releases.domain_name
    zone_id                = aws_cloudfront_distribution.releases.hosted_zone_id
    evaluate_target_health = false
  }
}

resource "aws_route53_record" "releases_aaaa" {
  zone_id = data.aws_route53_zone.drlinky.zone_id
  name    = local.subdomain
  type    = "AAAA"

  alias {
    name                   = aws_cloudfront_distribution.releases.domain_name
    zone_id                = aws_cloudfront_distribution.releases.hosted_zone_id
    evaluate_target_health = false
  }
}
