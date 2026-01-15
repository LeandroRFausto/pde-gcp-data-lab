#############################################
# Cloud Composer (opcional)
# Controlado por var.enable_composer
#############################################

# Obter o project number (sempre pode existir, é "data")
data "google_project" "project" {
  project_id = var.project_id
}

# Habilitar a API do Composer (opcional)
resource "google_project_service" "composer_api" {
  count              = var.enable_composer ? 1 : 0
  project            = var.project_id
  service            = "composer.googleapis.com"
  disable_on_destroy = false
}

# Service Account para o Composer (opcional)
resource "google_service_account" "composer_sa" {
  count        = var.enable_composer ? 1 : 0
  project      = var.project_id
  account_id   = "composer-orchestrator"
  display_name = "Composer Orchestrator Service Account"
}

# Dar permissão ao service agent do Composer (opcional)
# Necessário para Composer 2.x (roles/composer.ServiceAgentV2Ext)
resource "google_project_iam_member" "composer_service_agent" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"

  depends_on = [google_project_service.composer_api]
}

# Permissões necessárias para a SA do Composer (opcional)
resource "google_project_iam_member" "composer_worker" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa[0].email}"

  depends_on = [google_service_account.composer_sa]
}

# Para laboratório, evite bigquery.admin se possível.
# Mantenho como você tinha, mas considere trocar depois por roles mais restritas.
resource "google_project_iam_member" "composer_bigquery" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.composer_sa[0].email}"

  depends_on = [google_service_account.composer_sa]
}

resource "google_project_iam_member" "composer_storage" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.composer_sa[0].email}"

  depends_on = [google_service_account.composer_sa]
}

# Criar o ambiente Composer (Airflow) (opcional)
resource "google_composer_environment" "data_orchestrator" {
  count   = var.enable_composer ? 1 : 0
  name    = "data-pipeline-orchestrator"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      # Observação: esse valor pode variar conforme o provider/Composer.
      # Se der erro, ajustamos para uma versão suportada na sua região.
      image_version = "composer-2-airflow-2"

      env_variables = {
        BUCKET_RAW = "${var.project_id}-raw-data"
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1
        storage_gb = 1
        count      = 1
      }

      web_server {
        cpu        = 0.5
        memory_gb  = 1
        storage_gb = 1
      }

      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 2
      }
    }

    node_config {
      # "default" funciona na maioria dos casos, mas depende do provider.
      # Se você já usa VPC diferente, substitua aqui.
      network    = "projects/${var.project_id}/global/networks/default"
      subnetwork = "projects/${var.project_id}/regions/${var.region}/subnetworks/default"

      service_account = google_service_account.composer_sa[0].email
    }
  }

  depends_on = [
    google_project_service.composer_api,
    google_project_iam_member.composer_service_agent,
    google_project_iam_member.composer_worker,
    google_project_iam_member.composer_bigquery,
    google_project_iam_member.composer_storage
  ]
}

#############################################
# Outputs (opcionais e "safe" com count)
#############################################

output "airflow_ui_url" {
  description = "URL da interface web do Airflow (Composer)"
  value       = var.enable_composer ? google_composer_environment.data_orchestrator[0].config[0].airflow_uri : null
}

output "composer_dag_bucket" {
  description = "Caminho do bucket onde você deve colocar as DAGs (Composer)"
  value       = var.enable_composer ? google_composer_environment.data_orchestrator[0].config[0].dag_gcs_prefix : null
}
