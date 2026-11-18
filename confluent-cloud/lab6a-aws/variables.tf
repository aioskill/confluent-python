variable "region" {
  description = "AWS region of the bucket. Confluent requires the bucket to be in the same region as the Kafka cluster."
  type        = string
  default     = "ap-south-1"
}

variable "expire_days" {
  description = "Objects are deleted automatically after this many days, so forgotten lab data does not accumulate."
  type        = number
  default     = 2
}
