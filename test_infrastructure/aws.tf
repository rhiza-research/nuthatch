# AWS infrastructure for nuthatch integration tests

variable "aws_bucket_name" {
  description = "Name of the S3 test bucket"
  type        = string
  default     = "nuthatch-test"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS credentials profile"
  type        = string
  default     = "rhiza"
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

resource "aws_s3_bucket" "test" {
  bucket        = var.aws_bucket_name
  force_destroy = true
}

resource "aws_iam_user" "test" {
  name = "nuthatch-test"
}

resource "aws_iam_user_policy" "test" {
  name = "nuthatch-test-s3"
  user = aws_iam_user.test.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.test.arn,
          "${aws_s3_bucket.test.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_access_key" "test" {
  user = aws_iam_user.test.name
}

output "aws_bucket_name" {
  value = aws_s3_bucket.test.id
}

output "aws_access_key_id" {
  value = aws_iam_access_key.test.id
}

output "aws_secret_access_key" {
  value     = aws_iam_access_key.test.secret
  sensitive = true
}
