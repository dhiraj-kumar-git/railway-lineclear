# Passenger Notification's log group sends every new line to the log forwarder, which puts it in Loki

resource "aws_lambda_permission" "passenger_notification_logs" {
  statement_id  = "AllowPassengerNotificationLogs"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.log_forwarder.function_name
  principal     = "logs.amazonaws.com"
  source_arn    = "${aws_cloudwatch_log_group.passenger_notification.arn}:*"
}

resource "aws_cloudwatch_log_subscription_filter" "passenger_notification_to_loki" {
  name            = "to-loki"
  log_group_name  = aws_cloudwatch_log_group.passenger_notification.name
  filter_pattern  = "" # every line
  destination_arn = aws_lambda_function.log_forwarder.arn

  depends_on = [aws_lambda_permission.passenger_notification_logs]
}
