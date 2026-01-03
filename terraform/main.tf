terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.0.0"
}

provider "aws" {
  region = "eu-west-2"
}

# Example: S3 Bucket for Bronze Data
resource "aws_s3_bucket" "bronze_bucket" {
  bucket = "meditrack-bronze-bucket"
  force_destroy = true

  tags = {
    Environment = "dev"
    Project     = "MediTrack360"
  }
}

# Example: Redshift Serverless Namespace (Placeholder)
# resource "aws_redshiftserverless_namespace" "example" {
#   namespace_name = "meditrack-namespace"
# }

# Notes:
# - During development, infrastructure was provisioned via Homebrew.

