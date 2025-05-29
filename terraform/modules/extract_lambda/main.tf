module "extract_lambda" {
  source           = "../create_lambda"
  filename         = data.archive_file.extract_lambda_zip.output_path
  function_name    = var.lambda_name
  handler          = "${var.lambda_name}.lambda_handler"
  runtime          = var.python_runtime
  source_code_hash = data.archive_file.extract_lambda_zip.output_base64sha256
  environment_variables = {
    INGEST_ZONE_BUCKET_NAME = module.ingest_zone_bucket.s3_bucket.id
  }
  layers = [module.lambda_layer.aws_lambda_layer_version.arn]
}

data "archive_file" "extract_lambda_zip" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.root}/../src/lambdas/extract_lambda/extract_lambda.py"
  output_path      = "${path.root}/../dist/extract_lambda.zip"
}

module "lambda_layer" {
  source         = "../lambda_layer"
  python_runtime = var.python_runtime
}

module "ingest_zone_bucket" {
  source                = "../create_s3_bucket"
  s3_bucket_name_prefix = "ingest-bucket-"
}
