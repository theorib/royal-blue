variable "layer_zip_filename" {
  type    = string
  default = "layer.zip"
}

variable "layer_name" {
  type    = string
  default = "royal_blue_layer"
}
variable "python_runtime" {
  type = string
}

variable "build_script_file_path" {
  type    = string
  default = "./build_lambda_layer.sh"
}

variable "lambda_layers_bucket" {
  description = "Lambda S3 layers bucket ARN and ID."
  type = object({
    id  = string
    arn = string
  })
}
