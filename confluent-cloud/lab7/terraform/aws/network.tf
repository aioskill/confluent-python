# ---------------------------------------------------------------------------
# network.tf - the VPC that holds the database and the jump host
#
# Layout (10.20.0.0/16):
#
#   private-1/2/3  10.20.1-3.0/24   one per Availability Zone; the database,
#                                   the load balancer, and the ingress endpoint
#                                   live here. NO route to the internet.
#   public         10.20.100.0/24   the jump host only; routed to the internet
#                                   through an internet gateway.
#
# Manual equivalent: aws ec2 create-vpc / create-subnet / create-internet-gateway
# / create-route-table / associate-route-table.
# ---------------------------------------------------------------------------

locals {
  name = var.name_prefix

  # Confluent Cloud private clusters are reached through names under this
  # domain (see ingress.tf). It is fixed by Confluent; only the region varies.
  private_domain = "${var.region}.aws.private.confluent.cloud"
}

resource "aws_vpc" "lab" {
  cidr_block = "10.20.0.0/16"

  # Both flags are required so that private DNS names (the Route 53 zone in
  # ingress.tf, and RDS host names) resolve inside the VPC.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${local.name}-vpc" }
}

# Three private subnets, one per AZ. We use zone IDs (aps1-az1, ...) because
# Confluent's gateways are placed in specific zone IDs, and the load balancer
# and endpoints must cover the same zones.
resource "aws_subnet" "private" {
  count                = 3
  vpc_id               = aws_vpc.lab.id
  cidr_block           = "10.20.${count.index + 1}.0/24"
  availability_zone_id = var.az_ids[count.index]
  tags                 = { Name = "${local.name}-private-${count.index + 1}" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.lab.id
  cidr_block              = "10.20.100.0/24"
  availability_zone_id    = var.az_ids[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.name}-public" }
}

# Only the public subnet gets a route to the internet. Private subnets use the
# VPC's default route table, which only has the local 10.20.0.0/16 route, so
# nothing in them can reach (or be reached from) the internet.
resource "aws_internet_gateway" "lab" {
  vpc_id = aws_vpc.lab.id
  tags   = { Name = "${local.name}-igw" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.lab.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.lab.id
  }

  tags = { Name = "${local.name}-public" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}
