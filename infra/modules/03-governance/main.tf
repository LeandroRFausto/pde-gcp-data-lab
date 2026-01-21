# ------------------------------------------------------------
# GOVERNANCE MODULE
# - Dataform: habilita API + cria repositorio
# ------------------------------------------------------------

resource "google_project_service" "dataform_api" {
  count              = var.enable_dataform ? 1 : 0
  project            = var.project_id
  service            = "dataform.googleapis.com"
  disable_on_destroy = false
}

resource "google_dataform_repository" "iot_transformations" {
  count    = var.enable_dataform ? 1 : 0
  provider = google-beta

  project = var.project_id
  region  = var.region
  name    = "iot-transformations"

  depends_on = [google_project_service.dataform_api]
}
