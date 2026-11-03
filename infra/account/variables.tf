variable "project" {
  type    = string
  default = "lineclear"
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "budget_limit_usd" {
  type    = number
  default = 100
}

variable "budget_emails" {
  type = list(string)
}

output "state_bucket_name" {
  value = aws_s3_bucket.tfstate.bucket
}