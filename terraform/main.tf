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

module "etl_state_machine" {
  source = "./modules/step_function"
  name   = "etl-lambda-orchestration"
  type   = var.step_function_type

  lambda_arns = {
    extract   = module.extract_lambda.lambda.arn
    transform = module.transform_lambda.lambda.arn
  }
}

module "etl_scheduler" {
  source              = "./modules/scheduler"
  name                = "etl-sfn-scheduler"
  schedule_expression = "rate(20 minutes)"
  state_machine_arn   = module.etl_state_machine.state_machine["arn"]
}

module "extract_lambda" {
  source         = "./modules/extract_lambda"
  python_runtime = var.python_runtime
  s3_bucket = {
    arn = aws_s3_bucket.lambda_source_code.arn
    id  = aws_s3_bucket.lambda_source_code.id
  }
  lambda_layers_bucket = {
    arn = aws_s3_bucket.extract_lambda_layers.arn
    id  = aws_s3_bucket.extract_lambda_layers.id
  }
  ingest_zone_bucket = {
    arn = aws_s3_bucket.ingest_zone.arn
    id  = aws_s3_bucket.ingest_zone.id
  }
  lambda_state_bucket = {
    arn = aws_s3_bucket.lambda_state.arn
    id  = aws_s3_bucket.lambda_state.id
  }

  TOTESYS_DB_USER     = var.TOTESYS_DB_USER
  TOTESYS_DB_PASSWORD = var.TOTESYS_DB_PASSWORD
  TOTESYS_DB_HOST     = var.TOTESYS_DB_HOST
  TOTESYS_DB_DATABASE = var.TOTESYS_DB_DATABASE
  TOTESYS_DB_PORT     = var.TOTESYS_DB_PORT
}

module "transform_lambda" {
  source         = "./modules/transform_lambda"
  python_runtime = var.python_runtime
  s3_bucket = {
    arn = aws_s3_bucket.lambda_source_code.arn
    id  = aws_s3_bucket.lambda_source_code.id
  }
  lambda_layers_bucket = {
    arn = aws_s3_bucket.extract_lambda_layers.arn
    id  = aws_s3_bucket.extract_lambda_layers.id
  }
  ingest_zone_bucket = {
    arn = aws_s3_bucket.ingest_zone.arn
    id  = aws_s3_bucket.ingest_zone.id
  }
  lambda_state_bucket = {
    arn = aws_s3_bucket.lambda_state.arn
    id  = aws_s3_bucket.lambda_state.id
  }

  process_zone_bucket = {
    arn = aws_s3_bucket.process_zone.arn
    id  = aws_s3_bucket.process_zone.id
  }
}
