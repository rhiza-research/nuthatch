# Terraform and provider version requirements

terraform {
  required_version = ">= 1.13.3"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
    google = {
      source  = "hashicorp/google"
      version = ">= 7.0"
    }
    # azurerm = {
    #   source  = "hashicorp/azurerm"
    #   version = ">= 4.0"
    # }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = ">= 2.0"
    }
  }
}
