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

module "ingest_zone_bucket" {
  source = "./modules/create_s3_bucket"
  s3_bucket_name_prefix = "ingest-bucket-"
}

module "process_zone_bucket" {
  source = "./modules/create_s3_bucket"
  s3_bucket_name_prefix = "process-zone-bucket-"
}

module "state_bucket" {
  source = "./modules/create_s3_bucket"
  s3_bucket_name_prefix = "state-bucket-"
}
