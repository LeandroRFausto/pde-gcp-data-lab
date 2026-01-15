terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }

  backend "gcs" {
    bucket = "quick-cache-484111-j4-tfstate"
    prefix = "airflow"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
