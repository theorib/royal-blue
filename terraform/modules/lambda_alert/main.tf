resource "aws_cloudwatch_log_group" "critical_log_group" {
  name = "lambda_logs"

}

resource "aws_cloudwatch_log_metric_filter" "lambda_critical_errors" {
  name           = "${var.lambda_name}-critical-errors"
  pattern        = "CRITICAL"
  log_group_name = aws_cloudwatch_log_group.critical_log_group.name
  # log_group_name = "/aws/lambda/${var.lambda_name}"


  metric_transformation {
    name      = "${var.lambda_name}CriticalErrorCount"
    namespace = var.metric_namespace
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_critical_alarm" {
  alarm_name          = "${var.lambda_name}-critical-error-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm for CRITICAL errors in ${var.lambda_name} logs"
  alarm_actions       = [aws_sns_topic.lambda_alerts.arn]
}

resource "aws_sns_topic" "lambda_alerts" {
  name = "lambda-critical-alerts"
}

resource "aws_sns_topic_subscription" "email_subscriptions" {
  for_each  = toset(var.alert_emails)
  topic_arn = aws_sns_topic.lambda_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

