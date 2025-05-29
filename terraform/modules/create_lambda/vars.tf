variable "lambda_name" {
  type = string
}

variable "archive_file_source_path" {
  type = string
}

variable "archive_file_output_path" {
  type = string
}

variable "archive_file_excludes" {
  type    = list(string)
  default = []
}

variable "runtime" {
  type = string
  default = "python3.13"
}

variable "lambda_python_file_base_name" {
  type = string
}

variable "lambda_function_handler_name" {
    type = string
}

variable "environment_variables" {
  type    = map(string)
  default = {}
}

variable "lambda_layers" {
  type = list
  default = []
}