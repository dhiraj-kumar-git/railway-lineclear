# The staff API key: sent as the X-Staff-Key header on actions that change the line, such as resolving
# an incident or the demo fault switch. External Secrets copies it into the cluster; it is never in Git.
resource "random_password" "staff_api_key" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "staff_api_key" {
  name                    = "${var.project}/staff-api-key"
  recovery_window_in_days = 0 # deleted at once, so a rebuild can reuse the name
}

resource "aws_secretsmanager_secret_version" "staff_api_key" {
  secret_id     = aws_secretsmanager_secret.staff_api_key.id
  secret_string = jsonencode({ "api-key" = random_password.staff_api_key.result })
}
