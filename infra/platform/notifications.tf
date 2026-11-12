# Passenger Notification: Schedule Manager sends each committed diversion to an SQS queue; a Lambda
# function turns it into a notice in DynamoDB and a bulletin in S3, and serves both on a public HTTPS URL.

locals {
  notification_function = "${var.project}-passenger-notification"
}

# Messages the function has failed three times are moved here, to be looked at instead of retried forever
resource "aws_sqs_queue" "passenger_notifications_dead_letter" {
  name                      = "${var.project}-passenger-notifications-dead-letter"
  message_retention_seconds = 1209600 # 14 days, the longest SQS keeps a message
}

resource "aws_sqs_queue" "passenger_notifications" {
  name                       = "${var.project}-passenger-notifications"
  visibility_timeout_seconds = 60     # six times the function's timeout, as AWS advises for a Lambda trigger
  message_retention_seconds  = 345600 # 4 days
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.passenger_notifications_dead_letter.arn
    maxReceiveCount     = 3
  })
}

# Bulletins, one plain-text file per committed diversion. Private: passengers read them through the function
resource "aws_s3_bucket" "bulletins" {
  bucket        = "${var.project}-bulletins-${data.aws_caller_identity.current.account_id}"
  force_destroy = true # the teardown deletes the bulletins with the bucket
}

resource "aws_s3_bucket_public_access_block" "bulletins" {
  bucket                  = aws_s3_bucket.bulletins.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Created here rather than by Lambda, so it has a retention period and the teardown deletes it
resource "aws_cloudwatch_log_group" "passenger_notification" {
  name              = "/aws/lambda/${local.notification_function}"
  retention_in_days = 7
}

resource "aws_iam_role" "passenger_notification" {
  name = local.notification_function
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Only what the function does: read its queue, write and read notices and bulletins, write its logs
resource "aws_iam_role_policy" "passenger_notification" {
  name = "passenger-notification"
  role = aws_iam_role.passenger_notification.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadTheQueue"
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = aws_sqs_queue.passenger_notifications.arn
      },
      {
        Sid      = "Notices"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:Query"]
        Resource = [aws_dynamodb_table.passenger_notices.arn, "${aws_dynamodb_table.passenger_notices.arn}/index/*"]
      },
      {
        Sid      = "Bulletins"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject"]
        Resource = "${aws_s3_bucket.bulletins.arn}/bulletins/*"
      },
      {
        Sid      = "ListBulletins" # so a missing bulletin reads as "not found" rather than "access denied"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.bulletins.arn
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.passenger_notification.arn}:*"
      },
    ]
  })
}

# The function's code, zipped from the service folder on every plan. Kept inside the ignored .terraform folder
data "archive_file" "passenger_notification" {
  type        = "zip"
  source_dir  = "${path.module}/../../services/passenger-notification/app"
  output_path = "${path.module}/.terraform/passenger-notification.zip"
}

resource "aws_lambda_function" "passenger_notification" {
  function_name    = local.notification_function
  role             = aws_iam_role.passenger_notification.arn
  runtime          = "python3.13"
  handler          = "handler.handle"
  filename         = data.archive_file.passenger_notification.output_path
  source_code_hash = data.archive_file.passenger_notification.output_base64sha256 # a code change redeploys it
  timeout          = 10
  memory_size      = 256

  environment {
    variables = {
      NOTICES_TABLE    = aws_dynamodb_table.passenger_notices.name
      BULLETINS_BUCKET = aws_s3_bucket.bulletins.bucket
    }
  }

  depends_on = [aws_cloudwatch_log_group.passenger_notification, aws_iam_role_policy.passenger_notification]
}

# The SQS trigger. Lambda polls the queue and hands over up to 10 messages at a time; the function names
# the ones it could not use, and only those go back to the queue
resource "aws_lambda_event_source_mapping" "passenger_notifications" {
  event_source_arn        = aws_sqs_queue.passenger_notifications.arn
  function_name           = aws_lambda_function.passenger_notification.arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
}

# The passenger link: public and read-only, so no login
resource "aws_lambda_function_url" "passenger_notification" {
  function_name      = aws_lambda_function.passenger_notification.function_name
  authorization_type = "NONE"
}

# A public function URL needs both statements; without either, every request gets 403 Forbidden
resource "aws_lambda_permission" "function_url_public" {
  statement_id           = "AllowPublicFunctionUrl"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = aws_lambda_function.passenger_notification.function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

resource "aws_lambda_permission" "function_url_invoke" {
  statement_id             = "AllowInvokeThroughFunctionUrl"
  action                   = "lambda:InvokeFunction"
  function_name            = aws_lambda_function.passenger_notification.function_name
  principal                = "*"
  invoked_via_function_url = true # only through the URL, not a direct Invoke
}

# Schedule Manager may send to the queue, and nothing else
module "schedule_manager_pod_identity" {
  source  = "terraform-aws-modules/eks-pod-identity/aws"
  version = "~> 2.9"

  name                 = "${var.project}-schedule-manager"
  attach_custom_policy = true
  policy_statements = [
    {
      sid       = "PassengerNotifications"
      actions   = ["sqs:GetQueueUrl", "sqs:SendMessage"]
      resources = [aws_sqs_queue.passenger_notifications.arn]
    },
  ]

  associations = {
    schedule_manager = {
      cluster_name    = module.eks.cluster_name
      namespace       = "lineclear"
      service_account = "schedule-manager"
    }
  }
}
