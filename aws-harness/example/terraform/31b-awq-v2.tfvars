# Parallel v2 of the 31B-AWQ cluster — fully isolated VPC, EKS, IRSA, etc.
# Used to cut over traffic without touching the live wm-inf-31b-awq cluster.
# Delete this file (and `terraform workspace delete 31b-awq-v2`) once v1 is
# retired and you no longer need the parallel stack.
#
# cluster_name is intentionally short — the EKS module composes role names as
# "${cluster_name}-system-eks-node-group-" and AWS caps IAM name_prefix at 38
# chars. "wm-inf-31b-v2" (13) + suffix (23) = 36, safe.
#
# Usage:
#   make -f Makefile.31b-awq infra \
#     CLUSTER_NAME=wm-inf-31b-v2 \
#     TF_WORKSPACE=31b-awq-v2 \
#     TFVARS=31b-awq-v2.tfvars
cluster_name       = "wm-inf-31b-v2"
project            = "wm-inf-31b-v2"
aws_region         = "us-west-2"
gpu_capacity_type  = "ON_DEMAND"
gpu_instance_types = ["g6e.xlarge", "g6e.2xlarge"]
gpu_node_count     = 1
