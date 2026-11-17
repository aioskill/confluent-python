variable "region" {
  description = "The only AWS region the students may use. It must match the region of ../terraform/aws."
  type        = string
  default     = "ap-south-1"
}

variable "students" {
  description = "Student IDs. Each gets an IAM user lab7-<id> with one access key."
  type        = list(string)

  validation {
    condition     = alltrue([for s in var.students : can(regex("^[a-z][a-z0-9]{1,15}$", s))])
    error_message = "Student IDs must be lowercase letters and digits, 2-16 characters, starting with a letter."
  }
}

variable "instance_type" {
  description = "The only EC2 instance type students may launch (cost guardrail)."
  type        = string
  default     = "t3.micro"
}

variable "db_instance_class" {
  description = "The only RDS instance class students may create (cost guardrail)."
  type        = string
  default     = "db.t4g.micro"
}
