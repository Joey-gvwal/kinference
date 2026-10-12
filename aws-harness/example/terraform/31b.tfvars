# Single g6e.12xlarge node (4x L40S = 192 GB VRAM total, ~$391/day ON_DEMAND).
# Runs Gemma 4 31B bf16 with tensor-parallel-size=4 across all 4 GPUs.
# Separate cluster_name so both clusters can run in parallel without conflict.
# Used via Terraform workspace "31b" — separate state from default cluster.
#
# Usage:
#   make -f Makefile.31b infra   — creates/updates wm-inf-31b cluster
#   make -f Makefile.31b inference — deploys Gemma 4 31B with tensor parallelism
cluster_name       = "wm-inf-31b"
project            = "wm-inf-31b"
aws_region         = "us-west-2"
gpu_capacity_type  = "ON_DEMAND"
gpu_instance_types = ["g6e.12xlarge", "g6e.24xlarge"]
gpu_node_count     = 1
gpu_az             = "us-west-2b"
