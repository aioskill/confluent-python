# ---------------------------------------------------------------------------
# outputs.tf - values Terraform prints after `apply`
#
# Show them any time with `terraform output`. Sensitive ones are hidden; reveal
# one with `terraform output -json <name>`. The first two are inputs for ../aws.
# ---------------------------------------------------------------------------

output "egress_principal_arn" {
  description = "Give to ../aws as confluent_egress_principal_arn."
  value       = confluent_gateway.egress.aws_egress_private_link_gateway[0].principal_arn
}

output "ingress_service_name" {
  description = "Give to ../aws as confluent_ingress_service_name."
  value       = confluent_gateway.ingress.aws_ingress_private_link_gateway[0].vpc_endpoint_service_name
}

output "environment_id" {
  value = confluent_environment.lab.id
}

output "kafka_cluster_id" {
  value = confluent_kafka_cluster.lab.id
}

output "kafka_rest_endpoint" {
  description = "Private: resolves only inside the VPC (use it from the jump host)."
  value       = confluent_kafka_cluster.lab.rest_endpoint
}

output "kafka_bootstrap_endpoint" {
  value = confluent_kafka_cluster.lab.bootstrap_endpoint
}

output "connector_host" {
  description = "Use as connection.host in the connector config (NOT the RDS host). Empty until the egress endpoint exists."
  value       = try(confluent_access_point.egress[0].aws_egress_private_link_endpoint[0].vpc_endpoint_dns_name, "")
}

output "admin_kafka_api_key" {
  description = "Instructor key (CloudClusterAdmin) for creating topics and ACLs."
  value       = { key = confluent_api_key.admin.id, secret = confluent_api_key.admin.secret }
  sensitive   = true
}

output "student_kafka_api_keys" {
  description = "Each student's Kafka key. It has NO permissions until you create ACLs."
  value       = { for s, k in confluent_api_key.student : s => { key = k.id, secret = k.secret, service_account = confluent_service_account.student[s].id } }
  sensitive   = true
}
