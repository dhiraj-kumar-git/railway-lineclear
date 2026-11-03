terraform {
  backend "s3" {
    bucket       = "lineclear-tfstate-714989832567-ap-south-1"
    key          = "argocd/terraform.tfstate"
    region       = "ap-south-1"
    use_lockfile = true
    encrypt      = true
  }
}