# ------------------------------------------------------------
# Dataform (infra): habilita API + cria um repositório
# Observação: "workspace" de desenvolvimento é criado/initializado
# no Console do Dataform (não há resource no provider).
# ------------------------------------------------------------

# Habilita a API do Dataform
resource "google_project_service" "dataform_api" {
  project            = var.project_id
  service            = "dataform.googleapis.com"
  disable_on_destroy = false
}

# Repositório Dataform (precisa do provider google-beta)
resource "google_dataform_repository" "iot_transformations" {
  provider = google-beta

  project = var.project_id
  region  = var.region
  name    = "iot-transformations"

  depends_on = [google_project_service.dataform_api]
}

# (Opcional) Outputs úteis
output "dataform_repository_name" {
  value       = google_dataform_repository.iot_transformations.name
  description = "Nome do repositório Dataform"
}

output "dataform_repository_id" {
  value       = google_dataform_repository.iot_transformations.id
  description = "ID completo do repositório Dataform"
}
