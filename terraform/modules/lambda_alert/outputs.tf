output "cloudwatch_alarm_arn" {
  value = aws_cloudwatch_metric_alarm.lambda_critical_alarm.arn
}
