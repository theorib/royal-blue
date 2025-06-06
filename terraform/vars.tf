variable "python_runtime" {
  type    = string
  default = "python3.13"
}

variable "step_function_type" {
  description = "Step Function type: STANDARD or EXPRESS"
  type        = string
  default     = "STANDARD"
}

variable "TOTESYS_DB_USER" {
  description = "Totesys database user credentials"
  type        = string
  sensitive   = true
}
variable "TOTESYS_DB_PASSWORD" {
  description = "Totesys database user password credentials"
  type        = string
  sensitive   = true
}
variable "TOTESYS_DB_HOST" {
  description = "Totesys database host url"
  type        = string
  sensitive   = true
}
variable "TOTESYS_DB_DATABASE" {
  description = "Totesys database name"
  type        = string
  sensitive   = true
}
variable "TOTESYS_DB_PORT" {
  description = "Totesys database PostgreSQL port"
  type        = string
  sensitive   = true
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
