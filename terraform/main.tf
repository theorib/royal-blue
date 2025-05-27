terraform {
  backend "s3" {
    bucket = "royal-blue-terraform-state"
    key    = "projects/royal-blue"
    region = "eu-west-2"
  }
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}