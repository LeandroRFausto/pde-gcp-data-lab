resource "google_firestore_database" "database" {
  project     = var.project_id
  name        = "(default)"
  location_id = "nam5" # O Firestore é regional, mantivemos onde criamos
  type        = "FIRESTORE_NATIVE"

  # Evita que o Terraform destrua o banco se você destruir o resto da infra
  deletion_policy = "DELETE"
}
