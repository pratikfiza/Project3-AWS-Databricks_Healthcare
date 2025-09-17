terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "claims_bucket" {
  bucket = var.s3_bucket_name
  acl    = "private"
  # Add server-side encryption & versioning as needed
}

resource "aws_dynamodb_table" "metadata" {
  name         = "${var.prefix}-metadata"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "file_key"
  attribute {
    name = "file_key"
    type = "S"
  }
}

resource "aws_sns_topic" "alerts" {
  name = "${var.prefix}-alerts"
}

output "s3_bucket" {
  value = aws_s3_bucket.claims_bucket.id
}
