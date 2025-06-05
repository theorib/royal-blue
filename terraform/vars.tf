variable "python_runtime" {
  type    = string
  default = "python3.13"
}

variable "step_function_type" {
  description = "Step Function type: STANDARD or EXPRESS"
  type        = string
  default     = "STANDARD"
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
