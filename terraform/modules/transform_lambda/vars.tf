variable "function_name" {
  default = "transform_lambda"
}

variable "python_runtime" {
  type = string
}

variable "s3_bucket" {
  description = "Lambda S3 source code bucket ARN and ID."
  type = object({
    id  = string
    arn = string
  })
}
variable "lambda_layers_bucket" {
  description = "Lambda S3 layers bucket ARN and ID."
  type = object({
    id  = string
    arn = string
  })
}
variable "ingest_zone_bucket" {
  description = "Lambda S3 ingestion zone bucket ARN and ID. To be used internaly by lambda's source code via terraform injected environment variables and to create IAM permissions"
  type = object({
    id  = string
    arn = string
  })
}
variable "process_zone_bucket" {
  description = "Lambda S3 process zone bucket ARN and ID. To be used internaly by lambda's source code via terraform injected environment variables and to create IAM permissions"
  type = object({
    id  = string
    arn = string
  })
}

variable "lambda_state_bucket" {
  description = "Lambda S3 state storage bucket ARN and ID. To be used internaly by lambda's source code via terraform injected environment variables and to create IAM permissions"
  type = object({
    id  = string
    arn = string
  })
}

