# ---------------------------------------------------------------------------
# variables.tf - the inputs you can change
#
# Put values in terraform.tfvars (git-ignored); the README shows how to create
# it. Variables without a default must be set.
# ---------------------------------------------------------------------------

variable "region" {
  description = "AWS region. It must be the region of the Confluent gateways (../confluent) and enabled in your AWS account."
  type        = string
  default     = "ap-south-1"
}

variable "az_ids" {
  description = "Three Availability Zone IDs (not names; names like ap-south-1a map to different zones in different accounts)."
  type        = list(string)
  default     = ["aps1-az1", "aps1-az2", "aps1-az3"]
}

variable "name_prefix" {
  description = "Prefix for resource names. Give each student a different one (for example lab7-s07) when several stacks share one AWS account and region, because load balancer, database and key-pair names must be unique."
  type        = string
  default     = "lab7"

  validation {
    condition     = can(regex("^lab7[a-z0-9-]{0,20}$", var.name_prefix))
    error_message = "name_prefix must start with lab7 and use lowercase letters, digits and hyphens (max 25 characters)."
  }
}

variable "admin_cidr" {
  description = "Your public IP in CIDR form, for example 203.0.113.7/32. Only this address may SSH to the jump host."
  type        = string
}

variable "students" {
  description = "Student IDs. Each one gets a database schema and a read-only DB login. Use the same list as in ../confluent."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for s in var.students : can(regex("^[a-z][a-z0-9]{1,15}$", s))])
    error_message = "Student IDs must be lowercase letters and digits, 2-16 characters, starting with a letter."
  }
}

variable "acceptance_required" {
  description = "Require someone to approve Confluent's connection request to your VPC endpoint service. Leave false for a hands-off apply: the allowed-principal list already restricts who can connect."
  type        = bool
  default     = false
}

# --- Values produced by ../confluent (the Confluent half) ------------------

variable "confluent_egress_principal_arn" {
  description = "AWS identity of the Confluent egress gateway: ../confluent output egress_principal_arn. Only it may connect to the endpoint service."
  type        = string
}

variable "confluent_ingress_service_name" {
  description = "Name of the VPC endpoint service Confluent published for the ingress gateway: ../confluent output ingress_service_name."
  type        = string
}
