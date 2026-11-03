resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "10.11.6"
  namespace        = "argocd"
  create_namespace = true

  values = [yamlencode({
    dex = {
      enabled = false
    }
  })]
}

resource "helm_release" "root_app" {
  name      = "root-app"
  chart     = "${path.module}/root-app"
  namespace = "argocd"

  values = [yamlencode({
    repoURL        = var.gitops_repo_url
    targetRevision = var.gitops_revision
  })]

  depends_on = [helm_release.argocd]
}