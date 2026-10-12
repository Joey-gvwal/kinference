output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster API server endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_region" {
  description = "AWS region"
  value       = var.aws_region
}

output "kubeconfig_command" {
  description = "Run this to configure kubectl for the new cluster"
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${module.eks.cluster_name}"
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs used by EKS node groups"
  value       = module.vpc.private_subnets
}

output "aws_lbc_role_arn" {
  description = "IAM role ARN for the AWS Load Balancer Controller"
  value       = module.aws_lbc_irsa.iam_role_arn
}

output "linky_s3_role_arn" {
  description = "IAM role ARN for the Linky pod service account (direct-writes transcripts to S3)"
  value       = module.linky_s3_irsa.iam_role_arn
}
