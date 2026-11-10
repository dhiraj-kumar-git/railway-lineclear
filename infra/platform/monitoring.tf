resource "random_password" "grafana_admin" {
  length  = 24
  special = false
}

resource "aws_secretsmanager_secret" "grafana_admin" {
  name                    = "${var.project}/grafana/admin"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "grafana_admin" {
  secret_id = aws_secretsmanager_secret.grafana_admin.id
  secret_string = jsonencode({
    "admin-user"     = "admin"
    "admin-password" = random_password.grafana_admin.result
  })
}