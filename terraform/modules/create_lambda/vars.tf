variable "filename" {
  type        = string
  description = "(Optional) Path to the function's deployment package within the local filesystem. Exactly one of filename, image_uri, or s3_bucket must be specified."
}

variable "function_name" {
  type        = string
  description = "Unique name for your Lambda Function. (This is the name that will appear in your AWS Console"
}

variable "handler" {
  type        = string
  description = "Function entrypoint in your code. Normally it will be filename_base.function_name"
}

variable "runtime" {
  type        = string
  default     = "python3.13"
  description = "Identifier of the function's runtime"
}

variable "source_code_hash" {
  type        = string
  description = "(Optional) Virtual attribute used to trigger replacement when source code changes. Must be set to a base64-encoded SHA256 hash of the package file specified with either filename or s3_key."
}

variable "environment_variables" {
  type        = map(string)
  default     = {}
  description = "(Optional) Map of environment variables that are accessible from the function code during execution. If provided at least one key must be present."
}

variable "layers" {
  type        = list(any)
  default     = []
  description = "(Optional) List of Lambda Layer Version ARNs (maximum of 5) to attach to your Lambda Function."
}


