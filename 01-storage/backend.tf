terraform {
  backend "gcs" {
    bucket = "quick-cache-484111-j4-tfstate"
    prefix = "terraform/storage" 
  }
}
