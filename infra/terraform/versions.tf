terraform {
  required_version = ">= 1.10.0, < 2.0.0"

  # The operator supplies an existing encrypted, versioned state bucket and key.
  # `terraform init -backend=false` needs no AWS credentials.
  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.100"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}
