terraform {
  required_version = ">= 1.11.0"
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = "~> 3.0"
    }
  }
}

data "terraform_remote_state" "platform" {
  backend = "s3"
  config = {
    bucket = "lineclear-tfstate-714989832567-ap-south-1"
    key    = "platform/terraform.tfstate"
    region = "ap-south-1"
  }
}

locals {
  cluster = data.terraform_remote_state.platform.outputs
}

provider "helm" {
  kubernetes = {
    host                   = local.cluster.cluster_endpoint
    cluster_ca_certificate = base64decode(local.cluster.cluster_certificate_authority_data)
    exec = {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", local.cluster.cluster_name, "--region", var.region]
    }
  }
}