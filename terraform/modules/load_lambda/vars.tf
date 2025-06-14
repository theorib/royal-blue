variable "function_name" {
  default = "load_lambda"
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

variable "lambda_state_bucket" {
  description = "Lambda S3 state storage bucket ARN and ID. To be used internaly by lambda's source code via terraform injected environment variables and to create IAM permissions"
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

variable "DATAWAREHOUSE_DB_USER" {
  description = "Totesys database user credentials"
  type        = string
  sensitive   = true
}
variable "DATAWAREHOUSE_DB_PASSWORD" {
  description = "Totesys database user password credentials"
  type        = string
  sensitive   = true
}
variable "DATAWAREHOUSE_DB_HOST" {
  description = "Totesys database host url"
  type        = string
  sensitive   = true
}
variable "DATAWAREHOUSE_DB_DATABASE" {
  description = "Totesys database name"
  type        = string
  sensitive   = true
}
variable "DATAWAREHOUSE_DB_PORT" {
  description = "Totesys database PostgreSQL port"
  type        = string
  sensitive   = true
}
