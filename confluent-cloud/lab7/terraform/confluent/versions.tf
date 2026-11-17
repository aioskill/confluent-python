# ---------------------------------------------------------------------------
# versions.tf - which providers Terraform downloads (Confluent half of Lab 7)
#
# This directory needs ONLY a Confluent Cloud API key and no AWS credentials. It
# knows nothing about AWS resources except two values you pass in as variables
# once the AWS half exists (see variables.tf).
#
#   confluent  -> environment, gateways, cluster, identities, endpoints
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

# Confluent Cloud credentials come from two environment variables, so no secret
# is ever written in these files:
#
#   export CONFLUENT_CLOUD_API_KEY=...      # a *Cloud* API key
#   export CONFLUENT_CLOUD_API_SECRET=...   # (confluent api-key create --resource cloud)
provider "confluent" {}
