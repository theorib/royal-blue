resource "aws_sfn_state_machine" "state_machine" {
  name     = var.name
  role_arn = aws_iam_role.step_function_execution_role.arn
  type     = var.type

  definition = jsonencode({
    Comment = "Simple Step Function to invoke Lambda"
    StartAt = "extract"
    States = {
      extract = {
        Type     = "Task"
        Resource = var.lambda_arns["extract"]
        End      = true
      }

    }
  })
}