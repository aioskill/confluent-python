# ---------------------------------------------------------------------------
# s3.tf - the shared bucket
#
# One bucket for the whole class. Each student's connector writes under the
# folder "s3sink-<student id>/". Students share the bucket and the IAM user
# (iam.tf), so the folders separate their data by convention, not by permission.
#
# Manual equivalent: aws s3api create-bucket / put-public-access-block /
# put-bucket-encryption / put-bucket-lifecycle-configuration.
# ---------------------------------------------------------------------------

data "aws_caller_identity" "me" {}

locals {
  # Bucket names are global across all AWS accounts, so include the account ID.
  bucket = "lab6a-s3-${data.aws_caller_identity.me.account_id}-${var.region}"
}

resource "aws_s3_bucket" "lab" {
  bucket        = local.bucket
  force_destroy = true # let `terraform destroy` delete the bucket even if it holds objects
}

# Nothing in this bucket may ever be public, whatever a policy says.
resource "aws_s3_bucket_public_access_block" "lab" {
  bucket                  = aws_s3_bucket.lab.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Encrypt objects at rest with S3-managed keys (free, and the connector needs
# no extra permissions for it).
resource "aws_s3_bucket_server_side_encryption_configuration" "lab" {
  bucket = aws_s3_bucket.lab.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lab" {
  bucket = aws_s3_bucket.lab.id

  rule {
    id     = "expire-lab-data"
    status = "Enabled"
    filter {}

    expiration {
      days = var.expire_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
