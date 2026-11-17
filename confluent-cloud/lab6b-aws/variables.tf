variable "region" {
  description = "AWS region of the bucket. Confluent requires the bucket to be in the same region as the Kafka cluster."
  type        = string
  default     = "ap-south-1"
}

variable "students" {
  description = "Student IDs. Each gets an IAM user lab6b-<id> that can use only the <id>/ prefix of the bucket."
  type        = list(string)

  validation {
    condition     = alltrue([for s in var.students : can(regex("^[a-z][a-z0-9]{1,15}$", s))])
    error_message = "Student IDs must be lowercase letters and digits, 2-16 characters, starting with a letter."
  }
}

variable "expire_days" {
  description = "Objects are deleted automatically after this many days, so forgotten lab data does not accumulate."
  type        = number
  default     = 2
}
