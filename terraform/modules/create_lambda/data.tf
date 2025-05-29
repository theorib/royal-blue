data "aws_caller_identity" "current" {

}
data "aws_region" "current" {

}
data "archive_file" "lambda_function" {
  type        = "zip"
  source_dir  = var.archive_file_source_path
  output_path = var.archive_file_output_path
  excludes    = var.archive_file_excludes
}
