output "bucket_name" {
  description = "S3 bucket holding release artifacts — upload with `aws s3 cp ./file s3://$(bucket_name)/...`"
  value       = aws_s3_bucket.releases.id
}

output "distribution_id" {
  description = "CloudFront distribution ID — use with `aws cloudfront create-invalidation` after updating objects"
  value       = aws_cloudfront_distribution.releases.id
}

output "distribution_domain" {
  description = "Raw CloudFront domain (e.g. d123.cloudfront.net) — for sanity-checking before DNS propagates"
  value       = aws_cloudfront_distribution.releases.domain_name
}

output "public_url" {
  description = "Public HTTPS URL serving the bucket contents"
  value       = "https://releases.drlinky.buoyant.cloud"
}

output "certificate_arn" {
  description = "ACM certificate ARN (us-east-1) for releases.drlinky.buoyant.cloud"
  value       = aws_acm_certificate.releases.arn
}
