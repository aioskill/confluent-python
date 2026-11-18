# ---------------------------------------------------------------------------
# iam.tf - ONE shared IAM user for the whole class
#
# The managed S3 Sink connector runs in Confluent Cloud, outside your AWS
# account, so it needs an AWS identity to write with. All students share one
# IAM *user* with one access key (the connector's "username and password"), and
# a policy that allows writing only under "s3sink-<anything>/" in the bucket.
# Each student writes to their own folder "s3sink-<student id>/" by convention
# (the lab's topics.dir setting); IAM does not stop one student from touching
# another student's folder.
#
# Manual equivalent: aws iam create-user / put-user-policy / create-access-key.
# ---------------------------------------------------------------------------

resource "aws_iam_user" "class" {
  name          = "lab6a-students"
  force_destroy = true
  tags          = { purpose = "lab6a-students" }
}

data "aws_iam_policy_document" "class" {
  # The connector checks that the bucket exists and where it is. Listing bucket
  # names (ListAllMyBuckets) cannot be limited to one bucket in IAM.
  statement {
    sid       = "FindBucket"
    actions   = ["s3:ListAllMyBuckets", "s3:GetBucketLocation"]
    resources = ["*"]
  }

  # The connector's "does this bucket exist?" check (HeadBucket) needs
  # s3:ListBucket on the whole bucket. Do not put sensitive data in object names.
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.lab.arn]
  }

  # When it starts, the connector writes and deletes one test object with this
  # exact name at the bucket root, to prove that it can write.
  statement {
    sid       = "ConnectorWriteTest"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lab.arn}/__confluent_cloud_test___"]
  }

  # Reading, writing and deleting objects is allowed only in the lab's folders,
  # "s3sink-<student id>/". The multipart actions are what the connector uses to
  # upload large files.
  statement {
    sid = "ObjectsInLabFolders"
    actions = [
      "s3:PutObject", "s3:GetObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lab.arn}/s3sink-*/*"]
  }
}

resource "aws_iam_user_policy" "class" {
  name   = "lab6a-s3-lab-folders"
  user   = aws_iam_user.class.name
  policy = data.aws_iam_policy_document.class.json
}

resource "aws_iam_access_key" "class" {
  user = aws_iam_user.class.name
}

# One file for the whole class, ready to hand over privately. Keep the secret
# out of git and chat: it exists only here and in Terraform state (both
# git-ignored).
resource "local_sensitive_file" "credentials" {
  filename        = "${path.module}/student-credentials/s3.txt"
  file_permission = "0600"
  content         = <<-EOT
    s3_bucket=${aws_s3_bucket.lab.bucket}
    aws_region=${var.region}
    aws_access_key_id=${aws_iam_access_key.class.id}
    aws_secret_access_key=${aws_iam_access_key.class.secret}
  EOT
}
