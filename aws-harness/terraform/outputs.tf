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
  description = "Command to configure kubectl for this cluster"
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

output "weights_bucket" {
  description = "S3 bucket the GPU node IAM role can read for model weights"
  value       = var.weights_bucket
}

output "aws_lbc_role_arn" {
  description = "IAM role ARN for the AWS Load Balancer Controller (consumed by make expose-server)"
  value       = module.aws_lbc_irsa.iam_role_arn
}

output "gpu_node_group_name" {
  description = "Managed node group name to target with `make scale-gpu`"
  value       = "${var.cluster_name}-gpu"
}
