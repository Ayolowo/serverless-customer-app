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

# IAM role for aws Lambda to assume
resource "aws_iam_role" "iamrolelambda" {
  name               = "iamrolelambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_lambda_function" "terraform_function" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/dependencies/app.zip"
  function_name = "app_function"
  role          = aws_iam_role.iamrolelambda.arn
  handler       = "main.handler"

  runtime = "python3.12"
}

# Block to import RestAPI from AWS
import {
  to = aws_api_gateway_rest_api.app_function-API
  id = "re918jufnj"
}

resource "aws_api_gateway_rest_api" "app_function-API" {
  name = "app_function-API"

}

