variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "gitops_repo_url" {
  type    = string
  default = "https://github.com/dhiraj-kumar-git/railway-lineclear.git"
}

variable "gitops_revision" {
  type    = string
  default = "main"
}