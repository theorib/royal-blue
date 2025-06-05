variable "lambda_name" {
  default = "extract_lambda"
}
variable "python_runtime" {
  type = string
}

variable "ingestion_zone_bucket" {
  type = object({
    id  = string
    arn = string
  })
}

variable "lambda_state_bucket" {
  type = object({
    id  = string
    arn = string
  })
}

variable "DB_USER" {
  type      = string
  sensitive = true
}
variable "DB_PASSWORD" {
  type      = string
  sensitive = true
}
variable "DB_HOST" {
  type      = string
  sensitive = true
}
variable "DB_DATABASE" {
  type      = string
  sensitive = true
}
variable "DB_PORT" {
  type      = string
  sensitive = true
}
