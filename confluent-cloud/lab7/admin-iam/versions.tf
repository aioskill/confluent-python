# ---------------------------------------------------------------------------
# versions.tf - providers for the INSTRUCTOR-ONLY IAM configuration
#
# Run this configuration once, as an AWS administrator (a user that may create
# IAM users, policies and access keys). It creates the limited identities that
# students use to run ../terraform/aws. Students never run this directory.
#
#   aws    -> IAM users, group, policy, access keys
#   local  -> one credentials file per student (git-ignored)
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }
}

# Administrator credentials come from your normal AWS CLI configuration.
provider "aws" {
  region = var.region
}
