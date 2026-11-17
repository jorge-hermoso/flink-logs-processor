terraform {
  required_version = ">= 1.8.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
  }

  # Remote state is configured per environment, e.g.:
  #   tofu init -backend-config=environments/prod.s3.tfbackend
  # Keeping state local by default lets `tofu validate` run without AWS access.
  # backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = merge(var.tags, {
      Application = "production-logs-anomaly-detector"
      Environment = var.environment
      ManagedBy   = "opentofu"
    })
  }
}
