# ---------------------------------------------------------------------------
# egress.tf - how Confluent Cloud reaches the private database (PrivateLink)
#
# The path, left to right:
#
#   managed connector -> Confluent EGRESS endpoint ==PrivateLink==>
#     your VPC ENDPOINT SERVICE -> NETWORK LOAD BALANCER -> database (10.20.x.x)
#
# Why so many pieces? AWS PrivateLink can only publish a *Network Load Balancer*
# as a service, and RDS cannot be published directly, so the NLB forwards TCP
# port 5432 to the database's private IP.
#
# Manual equivalent: aws elbv2 create-load-balancer / create-target-group /
# register-targets / create-listener, aws ec2 create-vpc-endpoint-service-
# configuration, and aws ec2 modify-vpc-endpoint-service-permissions. (The
# matching Confluent egress endpoint is created in ../confluent.)
# ---------------------------------------------------------------------------

# An internal NLB (no public address) with one node in each private subnet.
# Cross-zone balancing is ON because the database lives in one AZ but the NLB
# has a node in each; without it, connections through the other nodes fail.
resource "aws_lb" "db" {
  name                             = "${local.name}-nlb"
  load_balancer_type               = "network"
  internal                         = true
  subnets                          = aws_subnet.private[*].id
  enable_cross_zone_load_balancing = true
}

# The target group is "where to forward": the database's private IP, port 5432.
# target_type = "ip" lets us use a raw address instead of an EC2 instance.
resource "aws_lb_target_group" "db" {
  name        = "${local.name}-db"
  port        = 5432
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = aws_vpc.lab.id
}

resource "aws_lb_target_group_attachment" "db" {
  target_group_arn = aws_lb_target_group.db.arn
  target_id        = data.aws_network_interface.db.private_ip # see database.tf
  port             = 5432
}

# The listener accepts TCP on 5432 and forwards to the target group.
resource "aws_lb_listener" "db" {
  load_balancer_arn = aws_lb.db.arn
  port              = 5432
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.db.arn
  }
}

# The VPC endpoint service is what turns the NLB into something another AWS
# account can connect to privately.
#
# allowed_principals is the security control: only the AWS identity that
# Confluent assigned to your egress gateway (principal_arn) may even request a
# connection. That value is created by ../confluent, so you pass it in as
# var.confluent_egress_principal_arn (see terraform.tfvars in the README).
#
# acceptance_required = false means an allowed principal connects without
# someone approving the request. Set var.acceptance_required = true to practise
# the manual approval (aws ec2 accept-vpc-endpoint-connections).
resource "aws_vpc_endpoint_service" "db" {
  acceptance_required        = var.acceptance_required
  network_load_balancer_arns = [aws_lb.db.arn]
  allowed_principals         = [var.confluent_egress_principal_arn]

  depends_on = [aws_lb_listener.db]
}

# DESTROY-ORDER FIX. After you destroy ../confluent, Confluent's API reports its
# endpoint "deleted" a few minutes before AWS has actually dropped its connection
# to the endpoint service, and AWS refuses to delete a service that still has a
# connection (ExistingVpcEndpointConnections). This resource does nothing on
# `apply`. On `destroy` it runs *before* the endpoint service is deleted and
# waits (polling AWS every 20 seconds, up
# to 30 minutes) until no connection remains. It needs the AWS CLI on the machine
# running Terraform.
resource "terraform_data" "wait_for_endpoint_disconnect" {
  input = {
    service_id = aws_vpc_endpoint_service.db.id
    region     = var.region
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      for i in $(seq 1 90); do
        n=$(aws ec2 describe-vpc-endpoint-connections --region ${self.input.region} \
              --filters Name=service-id,Values=${self.input.service_id} \
              --query 'VpcEndpointConnections[?VpcEndpointState!=`deleted`] | length(@)' \
              --output text)
        [ "$n" = "0" ] && exit 0
        echo "waiting for Confluent to disconnect from ${self.input.service_id} ($n connection(s))"
        sleep 20
      done
      echo "timed out waiting for the endpoint connection to be deleted"; exit 1
    EOT
  }
}
