output "bucket" {
  value = aws_s3_bucket.lab.bucket
}

output "student_users" {
  value = { for s, u in aws_iam_user.student : s => u.arn }
}

output "credentials_files" {
  description = "Hand each file to its student privately."
  value       = [for f in local_sensitive_file.credentials : f.filename]
}
