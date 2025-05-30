variable "name" {
  description = "Name of the schedule"
  type        = string
}

variable "schedule_expression" {
  description = "The schedule expression to trigger the Step Function"
  type        = string
  default     = "rate(20 minutes)"
}

variable "schedule_timezone" {
  description = "Timezone for the schedule"
  type        = string
  default     = "UTC"
}

variable "state_machine_arn" {
  description = "ARN of the target Step Function"
  type        = string
}
