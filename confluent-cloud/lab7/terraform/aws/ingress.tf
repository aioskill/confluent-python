# ---------------------------------------------------------------------------
# ingress.tf - how YOUR VPC reaches the private Confluent cluster
#
# An Enterprise cluster's addresses (lkc-xxxxx.<region>.aws.private.confluent.cloud)
# do not exist on the internet. To create topics and read data you need a private
# path from your VPC to Confluent Cloud. It has three parts:
#
#   1. a VPC *interface endpoint* in your VPC that connects to the service
#      Confluent published for your ingress gateway;
#   2. an *ingress endpoint* in Confluent Cloud that registers that VPC endpoint
#      (created by ../confluent, not here);
#   3. a DNS *private zone* so that the cluster's host names resolve to the
#      VPC endpoint's private addresses.
#
# Manual equivalent: aws ec2 create-vpc-endpoint, aws route53 create-hosted-zone
# and change-resource-record-sets. (Part 2, the Confluent ingress endpoint, is
# created in ../confluent.)
# ---------------------------------------------------------------------------

# Firewall for the VPC endpoint: HTTPS (REST API) and Kafka (9092) from inside
# the VPC only.
resource "aws_security_group" "ingress" {
  name        = "${local.name}-ingress-ep"
  description = "Lab 7 Confluent ingress endpoint"
  vpc_id      = aws_vpc.lab.id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.lab.cidr_block]
  }

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.lab.cidr_block]
  }
}

# (1) The VPC endpoint. The service name comes from the ingress gateway created
# in ../confluent (passed in as var.confluent_ingress_service_name).
# private_dns_enabled = false because Confluent's private DNS names are handled
# by our own Route 53 zone below.
resource "aws_vpc_endpoint" "ingress" {
  vpc_id              = aws_vpc.lab.id
  vpc_endpoint_type   = "Interface"
  service_name        = var.confluent_ingress_service_name
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.ingress.id]
  private_dns_enabled = false
}

# (2) Tell Confluent Cloud about the VPC endpoint so that it accepts traffic
# from it. That step happens in ../confluent (confluent_access_point.ingress),
# using this endpoint's id (output ingress_vpc_endpoint_id).

# (3) A Route 53 *private* hosted zone: it answers DNS questions only for
# resources inside the attached VPC. It covers the whole private.confluent.cloud
# domain for this region, so every cluster name resolves privately.
resource "aws_route53_zone" "private" {
  name = local.private_domain

  vpc {
    vpc_id = aws_vpc.lab.id
  }
}

# One wildcard record sends every name in the zone to the VPC endpoint. Confluent
# reads the host name you connect to (TLS "SNI") to pick the right cluster, which
# is why a single address can serve any cluster in the region.
#
# Gotcha: if you look up the cluster name BEFORE this record exists, the
# "not found" answer is cached by AWS DNS for up to 15 minutes. Terraform creates
# the record before anything uses the name, so do not test the name earlier.
resource "aws_route53_record" "wildcard" {
  zone_id = aws_route53_zone.private.zone_id
  name    = "*.${local.private_domain}"
  type    = "CNAME"
  ttl     = 60
  records = [aws_vpc_endpoint.ingress.dns_entry[0].dns_name]
}
