# ---------------------------------------------------------------------------
# confluent.tf - the Confluent Cloud side (part 1: no AWS values needed)
#
# Everything here is created through the Confluent Cloud API with your Cloud
# API key (CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET).
#
# Manual equivalents are the `confluent` CLI commands shown on each resource.
# ---------------------------------------------------------------------------

# The environment is the container for the gateways and the cluster. The
# Essentials governance package gives it a Schema Registry.
#   confluent environment create lab7-private --governance-package essentials
resource "confluent_environment" "lab" {
  display_name = "lab7-private"

  stream_governance {
    package = "ESSENTIALS"
  }
}

# EGRESS gateway: lets Confluent Cloud open connections OUT to your AWS account.
# The managed connector will reach the database through it. Confluent assigns
# the gateway an AWS identity (principal_arn); ../aws/egress.tf lets only that
# identity connect to your VPC endpoint service. You pass the value across with
# the output egress_principal_arn.
#   confluent network gateway create lab7-egress --cloud aws --region <region> --type egress-privatelink
resource "confluent_gateway" "egress" {
  display_name = "lab7-egress"

  environment {
    id = confluent_environment.lab.id
  }

  aws_egress_private_link_gateway {
    region = var.region
  }
}

# INGRESS gateway: lets YOUR VPC open connections IN to Confluent Cloud. An
# Enterprise cluster has no public endpoint, so this is how the jump host reaches
# it. Confluent publishes a VPC endpoint service (vpc_endpoint_service_name)
# that ../aws/ingress.tf connects to (output ingress_service_name).
#   confluent network gateway create lab7-ingress --cloud aws --region <region> --type ingress-privatelink
resource "confluent_gateway" "ingress" {
  display_name = "lab7-ingress"

  environment {
    id = confluent_environment.lab.id
  }

  aws_ingress_private_link_gateway {
    region = var.region
  }
}

# The Kafka cluster. Enterprise clusters are private-only and must be highly
# available (availability = HIGH); both are required for managed connectors to
# use PrivateLink. This is by far the most expensive resource in the lab.
#   confluent kafka cluster create lab7-enterprise --type enterprise --availability high ...
resource "confluent_kafka_cluster" "lab" {
  display_name = "lab7-enterprise"
  availability = "HIGH"
  cloud        = "AWS"
  region       = var.region

  enterprise {}

  environment {
    id = confluent_environment.lab.id
  }

  # Wait for the gateways first so that the cluster is created in an
  # environment that already has its private networking.
  depends_on = [confluent_gateway.egress, confluent_gateway.ingress]
}

# DESTROY-ORDER FIX. Connectors are created by students during the lab, outside
# Terraform, but Confluent will not delete a cluster that still has connectors
# ("The operation is not allowed for the cluster in its current state"). This
# resource does nothing on `apply`. On `destroy` it runs before the cluster is
# removed, lists the connectors on the cluster through the Cloud API (using the
# same CONFLUENT_CLOUD_API_KEY/SECRET that Terraform uses), deletes them all, and
# waits until the list is empty. Needs curl and python3 on the machine running
# Terraform.
#
# Manual equivalent: confluent connect cluster delete <id> for each connector.
resource "terraform_data" "delete_connectors" {
  input = {
    environment_id = confluent_environment.lab.id
    cluster_id     = confluent_kafka_cluster.lab.id
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      base="https://api.confluent.cloud/connect/v1/environments/${self.input.environment_id}/clusters/${self.input.cluster_id}/connectors"
      auth="$CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET"
      names=$(curl -s -u "$auth" "$base" | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)))')
      for n in $names; do
        echo "deleting connector $n"
        curl -s -o /dev/null -u "$auth" -X DELETE "$base/$n"
      done
      for i in $(seq 1 30); do
        left=$(curl -s -u "$auth" "$base" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
        [ "$left" = "0" ] && exit 0
        sleep 10
      done
      echo "connectors still present after 5 minutes"; exit 1
    EOT
  }

  depends_on = [confluent_kafka_cluster.lab]
}

# --- Identities for the lab ------------------------------------------------
#
# One service account + API key per student, and one administrator account that
# the instructor uses for the topic and ACL steps. An API key is the user
# name/password pair a program uses; its `owner` decides what it may do.

#   confluent iam service-account create lab7-<id>
resource "confluent_service_account" "student" {
  for_each     = toset(var.students)
  display_name = "lab7-${each.key}"
  description  = "Lab 7 student ${each.key}"
}

resource "confluent_service_account" "admin" {
  display_name = "lab7-admin"
  description  = "Lab 7 instructor: creates topics and ACLs"
}

# CloudClusterAdmin lets the admin account create topics and ACLs on this
# cluster (it is a role *binding*: who + which role + on what).
#   confluent iam rbac role-binding create --principal User:<sa> --role CloudClusterAdmin ...
resource "confluent_role_binding" "admin" {
  principal   = "User:${confluent_service_account.admin.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.lab.rbac_crn
}

# A Kafka API key per account, scoped to this one cluster. Creating a key gives
# it NO permissions by itself: the admin key gets its rights from the role
# binding above, and student keys get theirs from the ACLs you create in the lab.
#   confluent api-key create --resource <cluster> --service-account <sa>
resource "confluent_api_key" "student" {
  for_each     = confluent_service_account.student
  display_name = "lab7-${each.key}-kafka"

  # By default the provider confirms a new key works by listing topics on the
  # cluster. An Enterprise cluster is private, so that call cannot succeed from
  # the machine running Terraform and the apply would fail. Skip the check; the
  # key is usable from inside the VPC (the jump host).
  disable_wait_for_ready = true

  owner {
    id          = each.value.id
    api_version = each.value.api_version
    kind        = each.value.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.lab.id
    api_version = confluent_kafka_cluster.lab.api_version
    kind        = confluent_kafka_cluster.lab.kind

    environment {
      id = confluent_environment.lab.id
    }
  }
}

resource "confluent_api_key" "admin" {
  display_name = "lab7-admin-kafka"

  # By default the provider confirms a new key works by listing topics on the
  # cluster. An Enterprise cluster is private, so that call cannot succeed from
  # the machine running Terraform and the apply would fail. Skip the check; the
  # key is usable from inside the VPC (the jump host).
  disable_wait_for_ready = true

  owner {
    id          = confluent_service_account.admin.id
    api_version = confluent_service_account.admin.api_version
    kind        = confluent_service_account.admin.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.lab.id
    api_version = confluent_kafka_cluster.lab.api_version
    kind        = confluent_kafka_cluster.lab.kind

    environment {
      id = confluent_environment.lab.id
    }
  }

  depends_on = [confluent_role_binding.admin]
}
