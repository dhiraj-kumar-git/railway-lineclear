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

variable "enable_emr" {
  description = "Create the EMR cluster that runs the Flink job, with the load balancer that carries its logs to Loki. Leave false except while running or showing the job."
  type        = bool
  default     = false
}

variable "emr_instance_type" {
  type    = string
  default = "m5.xlarge" # the smallest general type EMR runs Flink on comfortably
}
