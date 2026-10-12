# dns.tf — Route 53 hosted zone + ACM certificate for drlinky.buoyant.cloud
#
# Run once with `make dns`.  These resources are cluster-independent and survive
# `make down` — do NOT put them in the cluster Terraform workspaces.
#
# After applying:
#   1. Add the output NS records to buoyant.cloud at your registrar as an NS
#      record set for the subdomain "linky-chat".
#   2. ACM will auto-validate the cert via DNS once the delegation propagates
#      (~5 min).  No further Terraform action needed.
#
# expose.sh looks up the cert by domain name at runtime, so the ARN never
# needs to be hardcoded anywhere.

locals {
  domain = "drlinky.buoyant.cloud"
}

# ── Route 53 hosted zone ──────────────────────────────────────────────────────
resource "aws_route53_zone" "linky_chat" {
  name = local.domain

  tags = {
    Name = local.domain
  }
}

# ── ACM certificate (DNS-validated) ──────────────────────────────────────────
# Intentionally no aws_acm_certificate_validation resource — Terraform would
# block here until the cert is issued, which requires NS delegation to be live
# first.  AWS validates it automatically once the delegation propagates.
resource "aws_acm_certificate" "linky_chat" {
  domain_name       = local.domain
  validation_method = "DNS"

  # Ensure the CAA record exists before ACM checks issuance permission.
  depends_on = [aws_route53_record.caa]

  # Allows re-creation (e.g. renewal) without downtime.
  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name = local.domain
  }
}

# ── CAA record ────────────────────────────────────────────────────────────────
# buoyant.cloud's CAA policy doesn't include Amazon's CA, so we add a record
# specifically for this subdomain.  Subdomain CAA records take precedence over
# the parent, so this doesn't affect anything else under buoyant.cloud.
resource "aws_route53_record" "caa" {
  zone_id = aws_route53_zone.linky_chat.zone_id
  name    = local.domain
  type    = "CAA"
  ttl     = 300
  records = [
    "0 issue \"amazon.com\"",
    "0 issuewild \"amazon.com\"",
  ]
}

# Add the CNAME record that ACM needs to validate domain ownership.
# Placed in our Route 53 zone so validation is fully automatic once NS
# delegation is active at the registrar.
resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.linky_chat.domain_validation_options : dvo.domain_name => {
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
  zone_id         = aws_route53_zone.linky_chat.zone_id
}
