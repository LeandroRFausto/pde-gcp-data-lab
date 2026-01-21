provider "google" {
  project      = var.project_id
  region       = var.region
  access_token = var.access_token
}

provider "google-beta" {
  project      = var.project_id
  region       = var.region
  access_token = var.access_token
}
