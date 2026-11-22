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
    configs = {
      # Team members reach the Argo CD page only through a port-forward their role allows
      # (gitops/cluster-config/team-access.yaml). Without signing in they can see every
      # Application and its sync state, but not sync, change or delete it: changes go through Git.
      cm = {
        "users.anonymous.enabled" = "true"
      }
      rbac = {
        "policy.default" = "role:readonly"
      }
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