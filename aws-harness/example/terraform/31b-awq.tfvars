# Single g6e.xlarge node (1x L40S = 48 GB VRAM, ~$53/day ON_DEMAND).
# Runs cyankiwi/gemma-4-31B-it-AWQ-4bit: ~16GB weights, no tensor parallelism.
# Same cost as the 26B cluster; full 31B dense model quality.
# Separate cluster_name and Terraform workspace from the FP8 31B cluster.
#
# Usage:
#   make -f Makefile.31b-awq find-az  — check AZ capacity first
#   make -f Makefile.31b-awq infra    — creates/updates wm-inf-31b-awq cluster
#   make -f Makefile.31b-awq inference  — deploys Gemma 4 31B AWQ-4bit
cluster_name       = "wm-inf-31b-awq"
project            = "wm-inf-31b-awq"
aws_region         = "us-west-2"
gpu_capacity_type  = "ON_DEMAND"
gpu_instance_types = ["g6e.xlarge", "g6e.2xlarge"]
gpu_node_count     = 1
