# ---------------------------------------------------------------------------
# versions.tf - which providers Terraform downloads (AWS half of Lab 7)
#
# This directory needs ONLY AWS credentials. It knows nothing about Confluent
# Cloud except three values you pass in as variables (see variables.tf).
#
#   aws        -> VPC, database, load balancer, endpoints, DNS
#   random     -> generates passwords (kept in state, never written in code)
#   tls/local  -> generates an SSH key pair and saves the private key to a file
#
# `terraform init` downloads these plugins into the hidden .terraform/ folder.
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

# AWS credentials come from your normal AWS CLI configuration
# (`aws sts get-caller-identity` must work). Students use the limited IAM user
# the instructor created (see ../../admin-iam).
provider "aws" {
  region = var.region
}
