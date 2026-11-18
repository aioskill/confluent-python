output "bucket" {
  value = aws_s3_bucket.lab.bucket
}

output "shared_user" {
  value = aws_iam_user.class.arn
}

output "credentials_file" {
  description = "Hand this file to every student privately."
  value       = local_sensitive_file.credentials.filename
}
