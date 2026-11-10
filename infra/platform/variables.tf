variable "project" {
  type    = string
  default = "lineclear"
}

variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "kubernetes_version" {
  type    = string
  default = "1.36"
}


variable "enable_msk" {
  description = "Create the MSK cluster. Set to false to leave Kafka out and save its hourly cost."
  type        = bool
  default     = true
}

variable "kafka_version" {
  type    = string
  default = "3.9.x"
}