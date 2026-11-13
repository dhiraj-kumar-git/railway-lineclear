# The log forwarder: a small Lambda function inside the VPC that pushes Lambda log lines from
# CloudWatch Logs into Loki through the internal load balancer, so they can be searched with the
# pods' logs. It is tooling for the logs, not one of the eight services.

locals {
  log_forwarder_function = "${var.project}-log-forwarder"
}

# Its own logs stay in CloudWatch only. Subscribing this group too would forward the forwarder's
# lines about forwarding, in a loop.
resource "aws_cloudwatch_log_group" "log_forwarder" {
  name              = "/aws/lambda/${local.log_forwarder_function}"
  retention_in_days = 7
}

# The function needs to reach Loki on port 3100 inside the VPC, and nothing else
resource "aws_security_group" "log_forwarder" {
  name        = local.log_forwarder_function
  description = "Log forwarder: sends log lines to Loki inside the VPC"
  vpc_id      = module.vpc.vpc_id
}

resource "aws_vpc_security_group_egress_rule" "log_forwarder_to_loki" {
  security_group_id = aws_security_group.log_forwarder.id
  cidr_ipv4         = module.vpc.vpc_cidr_block
  ip_protocol       = "tcp"
  from_port         = 3100
  to_port           = 3100
  description       = "Loki through the internal load balancer"
}

resource "aws_iam_role" "log_forwarder" {
  name = local.log_forwarder_function
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# AWS's managed policy for a function in a VPC: create its network interfaces and write its logs
resource "aws_iam_role_policy_attachment" "log_forwarder" {
  role       = aws_iam_role.log_forwarder.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

data "archive_file" "log_forwarder" {
  type        = "zip"
  source_dir  = "${path.module}/log-forwarder"
  output_path = "${path.module}/.terraform/log-forwarder.zip"
}

resource "aws_lambda_function" "log_forwarder" {
  function_name    = local.log_forwarder_function
  role             = aws_iam_role.log_forwarder.arn
  runtime          = "python3.13"
  handler          = "handler.handle"
  filename         = data.archive_file.log_forwarder.output_path
  source_code_hash = data.archive_file.log_forwarder.output_base64sha256
  timeout          = 10
  memory_size      = 128

  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.log_forwarder.id]
  }

  environment {
    variables = {
      LOKI_PUSH_URL = "http://${aws_lb.loki.dns_name}:3100/loki/api/v1/push"
      PROJECT       = var.project
    }
  }

  depends_on = [aws_cloudwatch_log_group.log_forwarder, aws_iam_role_policy_attachment.log_forwarder]
}
