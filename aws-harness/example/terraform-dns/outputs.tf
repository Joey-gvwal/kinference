output "name_servers" {
  description = "Add these as an NS record for 'linky-chat' at your buoyant.cloud registrar"
  value       = aws_route53_zone.linky_chat.name_servers
}

output "zone_id" {
  description = "Route 53 hosted zone ID for drlinky.buoyant.cloud"
  value       = aws_route53_zone.linky_chat.zone_id
}

output "certificate_arn" {
  description = "ACM certificate ARN for drlinky.buoyant.cloud"
  value       = aws_acm_certificate.linky_chat.arn
}
