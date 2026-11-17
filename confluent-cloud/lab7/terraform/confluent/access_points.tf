# ---------------------------------------------------------------------------
# access_points.tf - Confluent's end of the two PrivateLink connections
#
# These are the two resources that need something from AWS, so they are created
# in a SECOND apply, after ../aws exists. While the two variables are empty,
# count = 0 and nothing here is created.
#
# Manual equivalents:
#   confluent network access-point private-link egress-endpoint create ...
#   confluent network access-point private-link ingress-endpoint create ...
# ---------------------------------------------------------------------------

# EGRESS ENDPOINT: Confluent's side of the connection to your database. It asks
# AWS to connect to the VPC endpoint service built in ../aws/egress.tf. When it
# is READY, Confluent gives it a DNS name (output connector_host); the connector
# uses THAT as its database host instead of the RDS host name.
resource "confluent_access_point" "egress" {
  count        = var.egress_endpoint_service_name == "" ? 0 : 1
  display_name = "lab7-db-endpoint"

  environment {
    id = confluent_environment.lab.id
  }

  gateway {
    id = confluent_gateway.egress.id
  }

  aws_egress_private_link_endpoint {
    vpc_endpoint_service_name = var.egress_endpoint_service_name
    enable_high_availability  = true
  }

  # On destroy, connectors are deleted first (delete_connectors in confluent.tf),
  # then this endpoint, then the cluster.
  depends_on = [terraform_data.delete_connectors]
}

# INGRESS ENDPOINT: tells Confluent Cloud about the VPC endpoint you created in
# ../aws/ingress.tf, so that the cluster accepts traffic from it.
resource "confluent_access_point" "ingress" {
  count        = var.ingress_vpc_endpoint_id == "" ? 0 : 1
  display_name = "lab7-ingress-ep"

  environment {
    id = confluent_environment.lab.id
  }

  gateway {
    id = confluent_gateway.ingress.id
  }

  aws_ingress_private_link_endpoint {
    vpc_endpoint_id = var.ingress_vpc_endpoint_id
  }
}
