variable "project" {
  description = "Project tag applied to all AWS resources for cost attribution"
  type        = string
  default     = "inference"
}

variable "owner" {
  description = "Owner tag applied to all AWS resources for cost attribution"
  type        = string
  default     = "william@buoyant.io"
}
