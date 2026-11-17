# ---------------------------------------------------------------------------
# outputs.tf - values Terraform prints after `apply`
#
# Show them any time with `terraform output`. Sensitive ones are hidden; reveal
# one with `terraform output -raw <name>` or `terraform output -json <name>`.
# The first two are inputs for ../confluent (its access points).
# ---------------------------------------------------------------------------

output "egress_endpoint_service_name" {
  description = "Give to ../confluent as egress_endpoint_service_name."
  value       = aws_vpc_endpoint_service.db.service_name
}

output "ingress_vpc_endpoint_id" {
  description = "Give to ../confluent as ingress_vpc_endpoint_id."
  value       = aws_vpc_endpoint.ingress.id
}

output "rds_host" {
  description = "The database's real host name. Reachable only inside the VPC."
  value       = aws_db_instance.lab.address
}

output "ssh_command" {
  value = "ssh -i ${local_sensitive_file.jump_key.filename} ec2-user@${aws_instance.jump.public_ip}"
}

output "db_admin_password" {
  value     = random_password.db_admin.result
  sensitive = true
}

output "student_db_passwords" {
  description = "Each student's read-only database password."
  value       = { for s in var.students : s => random_password.student[s].result }
  sensitive   = true
}
