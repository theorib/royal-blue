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
        End      = true
      }

    }
  })
}
# {
#   "Comment": "Simple Step Function to invoke Lambda",
#   "StartAt": "extract",
#   "States": {
#     "extract": {
#       "Resource": "arn:aws:lambda:eu-west-2:228850271669:function:extract_lambda",
#       "Type": "Task",
#       "Next": "transform"
#     },
#     "transform": {
#       "Type": "Task",
#       "Resource": "arn:aws:states:::lambda:invoke",
#       "OutputPath": "$.Payload",
#       "Parameters": {
#         "Payload.$": "$",
#         "FunctionName": "arn:aws:lambda:eu-west-2:228850271669:function:transform_lambda:$LATEST"
#       },
#       "Retry": [
#         {
#           "ErrorEquals": [
#             "Lambda.ServiceException",
#             "Lambda.AWSLambdaException",
#             "Lambda.SdkClientException",
#             "Lambda.TooManyRequestsException"
#           ],
#           "IntervalSeconds": 1,
#           "MaxAttempts": 3,
#           "BackoffRate": 2,
#           "JitterStrategy": "FULL"
#         }
#       ],
#       "End": true
#     }
#   }
# }
