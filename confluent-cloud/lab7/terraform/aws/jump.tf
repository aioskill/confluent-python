# ---------------------------------------------------------------------------
# jump.tf - a small EC2 instance inside the VPC
#
# The database and the private Kafka cluster are unreachable from your laptop,
# so you SSH to this "jump host" and work from there. It is also used to seed
# the database with sample data when it first boots (jump-init.sh.tftpl).
#
# Manual equivalent: aws ec2 create-key-pair / create-security-group /
# run-instances.
# ---------------------------------------------------------------------------

# Generate an SSH key pair. Terraform keeps the private key in state and also
# writes it to lab7-key.pem (permissions 0600, git-ignored) so you can log in.
resource "tls_private_key" "jump" {
  algorithm = "ED25519"
}

resource "aws_key_pair" "jump" {
  key_name   = "${local.name}-key"
  public_key = tls_private_key.jump.public_key_openssh
}

resource "local_sensitive_file" "jump_key" {
  content         = tls_private_key.jump.private_key_openssh
  filename        = "${path.module}/${local.name}-key.pem"
  file_permission = "0600"
}

# Firewall: SSH only from your address (var.admin_cidr).
resource "aws_security_group" "jump" {
  name        = "${local.name}-jump"
  description = "Lab 7 jump host"
  vpc_id      = aws_vpc.lab.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Ask AWS for the current Amazon Linux 2023 image instead of hard-coding an ID.
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_instance" "jump" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = "t3.micro"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.jump.id]
  key_name               = aws_key_pair.jump.key_name

  # user_data is a script that runs once, as root, on first boot (cloud-init).
  # jump-init.sh.tftpl installs psql and the Kafka Python client, waits for the
  # database, and creates one schema + table + read-only user per student. The
  # script contains passwords, so Terraform hides it from plan output. Its log
  # is /var/log/lab7-init.log on the instance, and /var/log/lab7-seeded appears
  # when it finishes.
  user_data = sensitive(templatefile("${path.module}/jump-init.sh.tftpl", {
    db_host     = aws_db_instance.lab.address
    db_password = random_password.db_admin.result
    students    = { for s in var.students : s => random_password.student[s].result }
  }))

  tags = { Name = "${local.name}-jump" }
}
