terraform {
  required_version = ">= 1.11.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Application = "Railway LineClear"
      Project     = var.project
      ManagedBy   = "terraform"
      Stack       = "platform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  cluster_name = var.project

  services = [
    "train-registry",
    "position-ingestion",
    "incident-manager",
    "leader-election",
    "track-availability",
    "schedule-manager",
    "passenger-notification",
    "ops-analytics",
  ]

  service_databases = {
    "train-registry"     = "train_registry"
    "track-availability" = "track"
  }
}