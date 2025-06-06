resource "aws_lambda_function" "lambda" {
  s3_bucket        = var.s3_bucket.id
  s3_key           = var.extract_layer_zip_filename
  function_name    = var.function_name
  handler          = "src.lambdas.extract_lambda.lambda_handler"
  runtime          = var.python_runtime
  source_code_hash = filebase64sha256("${path.root}/../dist/${var.function_name}.zip")
  role             = aws_iam_role.iam_for_lambda.arn
  timeout          = 200
  environment {
    variables = {
      INGEST_ZONE_BUCKET_NAME  = var.ingest_zone_bucket.id
      LAMBDA_STATE_BUCKET_NAME = var.lambda_state_bucket.id
      DB_USER                  = var.DB_USER
      DB_PASSWORD              = var.DB_PASSWORD
      DB_HOST                  = var.DB_HOST
      DB_DATABASE              = var.DB_DATABASE
      DB_PORT                  = var.DB_PORT
    }
  }

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

resource "aws_s3_object" "lambda_src_object" {
  bucket     = var.s3_bucket.id
  key        = var.extract_layer_zip_filename
  source     = "${path.root}/../dist/${var.function_name}.zip"
  etag       = filebase64sha256("${path.root}/../dist/${var.function_name}.zip")
  depends_on = [var.s3_bucket]
}




