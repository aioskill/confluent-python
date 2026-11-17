# ---------------------------------------------------------------------------
# users.tf - one IAM user per student, all in one group
#
# Attaching the policy to a GROUP (not to each user) means a permission change
# is made in one place. Each user gets one access key (the "username and
# password" for the AWS CLI and Terraform). There is no console password: the
# students only use the command line.
#
# Manual equivalent: aws iam create-group / put-group-policy (attach-group-
# policy) / create-user / add-user-to-group / create-access-key.
# ---------------------------------------------------------------------------

resource "aws_iam_group" "students" {
  name = "lab7-students"
}

resource "aws_iam_group_policy_attachment" "students" {
  group      = aws_iam_group.students.name
  policy_arn = aws_iam_policy.student.arn
}

resource "aws_iam_user" "student" {
  for_each      = toset(var.students)
  name          = "lab7-${each.key}"
  force_destroy = true # also removes keys created outside Terraform
  tags          = { purpose = "lab7-student" }
}

resource "aws_iam_user_group_membership" "student" {
  for_each = aws_iam_user.student
  user     = each.value.name
  groups   = [aws_iam_group.students.name]
}

resource "aws_iam_access_key" "student" {
  for_each = aws_iam_user.student
  user     = each.value.name
}

# One file per student, ready to hand over privately. The secret key exists only
# here and in Terraform state, so protect both (both are git-ignored).
resource "local_sensitive_file" "credentials" {
  for_each        = aws_iam_access_key.student
  filename        = "${path.module}/student-credentials/${each.key}.txt"
  file_permission = "0600"
  content         = <<-EOT
    # AWS credentials for Lab 7, student ${each.key}. Do not share or commit.
    # Run these four commands once on your machine:
    aws configure set aws_access_key_id ${each.value.id}
    aws configure set aws_secret_access_key ${each.value.secret}
    aws configure set region ${var.region}
    aws sts get-caller-identity
  EOT
}
