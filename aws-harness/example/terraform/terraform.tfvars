# Local overrides — edit this file to change settings without touching variables.tf
# This file is intentionally not committed to version control (add to .gitignore).

cluster_name       = "wm-inference" # k8s cluster name
project            = "wm-inference" # used for AWS tags
aws_region         = "us-west-2"
gpu_capacity_type  = "ON_DEMAND"                        # Switch back to "SPOT" once quota is approved
gpu_instance_types = ["g6e.xlarge", "g6e.2xlarge"]     # L40S 48 GB VRAM — required for Gemma 4
