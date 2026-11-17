# ---------------------------------------------------------------------------
# database.tf - the private PostgreSQL database
#
# The database has no public address (publicly_accessible = false) and lives
# only in the private subnets. Nothing outside the VPC can reach it, which is
# the whole point of the lab: Confluent Cloud will get to it through PrivateLink.
#
# Manual equivalent: aws rds create-db-subnet-group / create-db-instance
# --no-publicly-accessible.
# ---------------------------------------------------------------------------

# Passwords are generated here and stored only in Terraform state. Read them with
#   terraform output -raw db_admin_password
#   terraform output -json student_db_passwords
resource "random_password" "db_admin" {
  length  = 20
  special = false
}

resource "random_password" "student" {
  for_each = toset(var.students)
  length   = 16
  special  = false
}

# Security group = the database's firewall. Port 5432 is open to the whole VPC
# (10.20.0.0/16), not to a single address, because traffic that arrives through
# PrivateLink comes from the load balancer's private addresses (egress.tf), and
# those can change.
resource "aws_security_group" "db" {
  name        = "${local.name}-db"
  description = "Lab 7 database"
  vpc_id      = aws_vpc.lab.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.lab.cidr_block]
  }

  # Terraform removes AWS's default "allow all outbound" rule, so add it back.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# RDS needs a subnet group covering at least two AZs.
resource "aws_db_subnet_group" "lab" {
  name       = "${local.name}-db"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_instance" "lab" {
  identifier             = "${local.name}-postgres"
  engine                 = "postgres"
  instance_class         = "db.t4g.micro" # smallest class; this is a lab
  allocated_storage      = 20             # GB
  db_name                = "labdb"
  username               = "labadmin"
  password               = random_password.db_admin.result
  db_subnet_group_name   = aws_db_subnet_group.lab.name
  vpc_security_group_ids = [aws_security_group.db.id]

  publicly_accessible = false # <- no public address

  # Lab settings: no backups, no final snapshot, so `terraform destroy` is fast
  # and leaves nothing behind. Do not copy these to a real database.
  backup_retention_period = 0
  skip_final_snapshot     = true
  apply_immediately       = true
}

# The load balancer in egress.tf must point at the database's *private IP*, but
# RDS only gives you a DNS name. RDS creates a network interface (ENI) in your
# subnets that carries the database's address, tagged with the database's
# security group. This looks that interface up so we can read its private IP.
#
# Caveat: if RDS later moves the database (failover, restore, maintenance), the
# IP changes. `terraform apply` refreshes it; production setups automate this.
data "aws_network_interfaces" "db" {
  filter {
    name   = "vpc-id"
    values = [aws_vpc.lab.id]
  }
  filter {
    name   = "group-id"
    values = [aws_security_group.db.id]
  }
  depends_on = [aws_db_instance.lab] # the ENI only exists once the DB exists
}

data "aws_network_interface" "db" {
  id = data.aws_network_interfaces.db.ids[0]
}
