variable "name" {
  description = "Name of the Step Function"
  type        = string
}

variable "lambda_arns" {
  description = "ARN(s) of the Lambda functions this Step Function should invoke in sequence"
  type        = map(string)
}

variable "type" {
  description = "Step Function type: STANDARD or EXPRESS"
  type        = string
  default     = "STANDARD"
}