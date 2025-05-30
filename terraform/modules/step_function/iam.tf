resource "aws_iam_role" "step_function_execution_role" {
  name = "etl_step_function_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "states.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "invoke_lambda" {
  name = "allow-step-function-lambda"
  role = aws_iam_role.step_function_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = "lambda:InvokeFunction",
      Resource = "*"
    }]
  })
}
