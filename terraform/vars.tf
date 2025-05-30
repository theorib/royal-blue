variable "python_runtime" {
  type    = string
  default = "python3.13"
}

variable "step_function_type" {
  description = "Step Function type: STANDARD or EXPRESS"
  type        = string
  default     = "STANDARD"
}