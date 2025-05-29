resource "aws_lambda_function" "generated_lambda" {
    filename      = data.archive_file.lambda_function.output_path

    function_name = var.lambda_name
    role          = aws_iam_role.iam_for_lambda.arn
    handler       = "${var.lambda_python_file_base_name}.${var.lambda_function_handler_name}"

    source_code_hash = data.archive_file.lambda_function.output_base64sha256

    runtime = var.runtime

    layers = var.lambda_layers

    
    environment {
    variables = var.environment_variables
    }
}