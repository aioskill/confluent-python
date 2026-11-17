# ---------------------------------------------------------------------------
# variables.tf - the inputs you can change
#
# Put values in terraform.tfvars (git-ignored); the README shows how to create
# it. The two endpoint variables stay empty until the AWS half exists.
# ---------------------------------------------------------------------------

variable "region" {
  description = "AWS region for the gateways and the cluster. It must be the region of ../aws and supported by Confluent Cloud."
  type        = string
  default     = "ap-south-1"
}

variable "students" {
  description = "Student IDs. Each one gets a Confluent service account and a Kafka API key limited (by the ACLs you create in the lab) to names starting with the ID. Use the same list as in ../aws."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for s in var.students : can(regex("^[a-z][a-z0-9]{1,15}$", s))])
    error_message = "Student IDs must be lowercase letters and digits, 2-16 characters, starting with a letter."
  }
}

# --- Values produced by ../aws (the AWS half) ------------------------------
# Leave both empty on the first apply. After ../aws is applied, set them and
# apply again: that creates the two Confluent endpoints (access_points.tf).

variable "egress_endpoint_service_name" {
  description = "../aws output egress_endpoint_service_name (the VPC endpoint service in front of the database)."
  type        = string
  default     = ""
}

variable "ingress_vpc_endpoint_id" {
  description = "../aws output ingress_vpc_endpoint_id (the VPC endpoint that connects your VPC to the cluster)."
  type        = string
  default     = ""
}
