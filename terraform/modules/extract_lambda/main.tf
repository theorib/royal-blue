module "extract_lambda" {
  source = "../create_lambda"
  # filename         = data.archive_file.extract_lambda_zip.output_path
  # filename         = "${path.root}/../dist/${var.lambda_name}.zip"
  s3_bucket        = aws_s3_bucket.extract_lambda_source_code.id
  s3_key           = var.extract_layer_zip_filename
  function_name    = var.lambda_name
  handler          = "src.lambdas.extract_lambda.lambda_handler"
  runtime          = var.python_runtime
  source_code_hash = filebase64sha256("${path.root}/../dist/${var.lambda_name}.zip")
  # source_code_hash = data.archive_file.extract_lambda_zip.output_base64sha256
  environment_variables = {
    INGEST_ZONE_BUCKET_NAME  = var.ingestion_zone_bucket.id
    LAMBDA_STATE_BUCKET_NAME = var.lambda_state_bucket.id
    DB_USER                  = var.DB_USER
    DB_PASSWORD              = var.DB_PASSWORD
    DB_HOST                  = var.DB_HOST
    DB_DATABASE              = var.DB_DATABASE
    DB_PORT                  = var.DB_PORT
  }
  # layers = [aws_lambda_layer_version.extract_lambda.arn]
  layers     = [aws_lambda_layer_version.extract_lambda.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:1"]
  depends_on = [resource.null_resource.create_extract_lambda_build, aws_s3_object.lambda_src_object, aws_s3_object.lambda_layer_object]
}

resource "null_resource" "create_extract_lambda_build" {
  provisioner "local-exec" {
    working_dir = "${path.root}/../"
    command     = "./build_extract_lambda_zip.sh"
  }
  triggers = {
    lock_file_hash = filebase64sha256("${path.root}/../uv.lock")
    src_hash       = sha256(join("", [for f in fileset("${path.root}/../src", "**") : filesha256("${path.root}/../src/${f}")]))
  }
}

resource "null_resource" "extract_lambda_layer_build" {
  provisioner "local-exec" {
    working_dir = "${path.root}/../"
    command     = "./build_lambda_layer.sh"
  }
  triggers = {
    lock_file_hash = filebase64sha256("${path.root}/../uv.lock")
  }
}


resource "aws_s3_object" "lambda_src_object" {
  bucket     = aws_s3_bucket.extract_lambda_source_code.id
  key        = var.extract_layer_zip_filename
  source     = "${path.root}/../dist/${var.lambda_name}.zip"
  etag       = filebase64sha256("${path.root}/../dist/${var.lambda_name}.zip")
  depends_on = [aws_s3_bucket.extract_lambda_source_code]
}


# resource "aws_lambda_layer_version" "lambda_layer" {
#   for_each = { for lib in var.libraries : lib.name => lib }

#   layer_name          = each.value.name
#   s3_bucket           = aws_s3_bucket.lambda_layer_bucket.id
#   s3_key              = "${each.value.name}.zip"
#   compatible_runtimes = var.layer_compatible_runtimes

#   depends_on = [aws_s3_object.lambda_layer_object]
# }



variable "extract_layer_zip_filename" {
  default = "layer.zip"
}

data "archive_file" "extract_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.root}/../build/layer"
  output_path      = "${path.root}/../dist/${var.extract_layer_zip_filename}"
  depends_on       = [null_resource.extract_lambda_layer_build]
}


resource "aws_s3_object" "lambda_layer_object" {
  # for_each = { for lib in var.libraries : lib.name => lib }

  bucket = aws_s3_bucket.extract_lambda_layers.id
  key    = var.extract_layer_zip_filename
  source = data.archive_file.extract_layer.output_path
  etag   = data.archive_file.extract_layer.output_sha256
}

resource "aws_lambda_layer_version" "extract_lambda" {
  layer_name          = "royal_blue_layer"
  s3_bucket           = aws_s3_bucket.extract_lambda_layers.id
  s3_key              = aws_s3_object.lambda_layer_object.key
  compatible_runtimes = [var.python_runtime]
  # filename            = data.archive_file.extract_layer.output_path
  source_code_hash = data.archive_file.extract_layer.output_base64sha256
  lifecycle {
    create_before_destroy = true
  }
}


# resource "aws_serverlessapplicationrepository_cloudformation_stack" "aws_sdk_pandas_layer" {
#   name           = "aws-sdk-pandas-layer-3-13"
#   application_id = "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:1"
#   capabilities = [
#     "CAPABILITY_IAM"
#   ]
# }


# data "archive_file" "extract_lambda_zip" {
#   type             = "zip"
#   output_file_mode = "0666"
#   source_dir       = "${path.root}/../build/extract_lambda/"
#   # source_file      = "${path.root}/../src/lambdas/extract_lambda/extract_lambda.py"
#   output_path = "${path.root}/../dist/extract_lambda.zip"

# }




