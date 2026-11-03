terraform {
  required_version = ">= 1.4" # terraform_data / replace_triggered_by

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.30.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}

provider "confluent" {
  # Credentials are NOT stored anywhere. ../start.sh prompts for them and exports:
  #   CONFLUENT_CLOUD_API_KEY    = "XXXXX"
  #   CONFLUENT_CLOUD_API_SECRET = "XXXXX"
  # (a Cloud resource management API key, see ../README.md)
  # If you run terraform by hand, export these two variables first.
}
