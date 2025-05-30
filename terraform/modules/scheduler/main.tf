resource "aws_scheduler_schedule" "schedule" {
  name       = var.name
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone 

  target {
    arn      = var.state_machine_arn
    role_arn = aws_iam_role.scheduler_role.arn
    input = jsonencode({
      source = "scheduler"
    })
  }
}