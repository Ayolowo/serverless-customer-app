# Create a lambda function to run python code
data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "iam_for_lambda" {
  name               = "iam_for_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/app/app.py"
  output_path = "${path.module}/app/app.zip"
}

resource "aws_lambda_function" "terraform_function" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/app/app.zip"
  function_name = "movies_lambda_function"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "app.lambda_handler"

  source_code_hash = data.archive_file.lambda.output_base64sha256

  runtime    = "python3.12"
  depends_on = [aws_iam_role.iam_for_lambda]
}

# To get logs of our lambda functions | retention days for how long to keep logs
# resource "aws_cloudwatch_log_group" "lambda_log_group" {
#   name              = "/aws/lambda/hello_world_lambda"
#   retention_in_days = 14
# }
