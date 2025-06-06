resource "aws_sfn_state_machine" "state_machine" {
  name     = var.name
  role_arn = aws_iam_role.step_function_execution_role.arn
  type     = var.type

  definition = jsonencode({
    Comment = "ETL Totesys DB data Pipeline"
    StartAt = "extract"
    States = {
      extract = {
        Type     = "Task"
        Resource = var.lambda_arns["extract"]
        Next     = "transform"
      }
      transform = {
        Type     = "Task"
        Resource = var.lambda_arns["transform"]
        Next     = "load"
      }
      load = {
        Type     = "Task"
        Resource = var.lambda_arns["load"]
        End      = true
      }
    }
  })
}
