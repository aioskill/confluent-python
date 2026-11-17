# ---------------------------------------------------------------------------
# iam.tf - one IAM user per student, limited to their own prefix
#
# The managed S3 Sink connector runs in Confluent Cloud, outside your AWS
# account, so it needs an AWS identity to write with. Each student gets an IAM
# *user* with an access key (the connector's "username and password"), and a
# policy that allows writing only under "<student id>/" in the shared bucket.
#
# Manual equivalent: aws iam create-user / put-user-policy / create-access-key.
# ---------------------------------------------------------------------------

resource "aws_iam_user" "student" {
  for_each      = toset(var.students)
  name          = "lab6b-${each.key}"
  force_destroy = true
  tags          = { purpose = "lab6b-student" }
}

data "aws_iam_policy_document" "student" {
  for_each = toset(var.students)

  # The connector checks that the bucket exists and where it is. Listing bucket
  # names (ListAllMyBuckets) cannot be limited to one bucket in IAM.
  statement {
    sid       = "FindBucket"
    actions   = ["s3:ListAllMyBuckets", "s3:GetBucketLocation"]
    resources = ["*"]
  }

  # The connector's "does this bucket exist?" check (HeadBucket) needs
  # s3:ListBucket on the whole bucket: AWS denies it if the policy limits
  # listing to a prefix. So students can LIST every key name in the bucket, but
  # they can read, write or delete objects only under their own prefix (next
  # statement). Do not put sensitive data in object names.
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.lab.arn]
  }

  # When it starts, the connector writes and deletes one test object with this
  # exact name at the bucket root, to prove that it can write. Allow that single
  # key (it holds no data and is shared by all students).
  statement {
    sid       = "ConnectorWriteTest"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lab.arn}/__confluent_cloud_test___"]
  }

  # Reading, writing and deleting objects is allowed only under this prefix.
  # The multipart actions are what the connector uses to upload large files.
  statement {
    sid = "ObjectsInOwnPrefix"
    actions = [
      "s3:PutObject", "s3:GetObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lab.arn}/${each.key}/*"]
  }
}

resource "aws_iam_user_policy" "student" {
  for_each = aws_iam_user.student
  name     = "lab6b-s3-own-prefix"
  user     = each.value.name
  policy   = data.aws_iam_policy_document.student[each.key].json
}

resource "aws_iam_access_key" "student" {
  for_each = aws_iam_user.student
  user     = each.value.name
}

# One file per student, ready to hand over privately. Keep the secret out of
# git and chat: it exists only here and in Terraform state (both git-ignored).
resource "local_sensitive_file" "credentials" {
  for_each        = aws_iam_access_key.student
  filename        = "${path.module}/student-credentials/${each.key}.txt"
  file_permission = "0600"
  content         = <<-EOT
    student=${each.key}
    s3_bucket=${aws_s3_bucket.lab.bucket}
    s3_prefix=${each.key}/
    aws_region=${var.region}
    aws_access_key_id=${each.value.id}
    aws_secret_access_key=${each.value.secret}
  EOT
}
