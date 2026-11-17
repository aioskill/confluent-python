# ---------------------------------------------------------------------------
# versions.tf - providers for the INSTRUCTOR-ONLY AWS setup of Lab 6B
#
# Run this configuration once, as an AWS administrator. It creates the S3 bucket
# that the managed S3 Sink connector writes to, and one IAM user per student who
# can write only under their own prefix of that bucket. Students never run it.
#
#   aws    -> S3 bucket, IAM users, policies, access keys
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
