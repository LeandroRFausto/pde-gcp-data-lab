# Obter o project number
data "google_project" "project" {
  project_id = var.project_id
}

# Habilitar a API do Composer
resource "google_project_service" "composer_api" {
  project = var.project_id
  service = "composer.googleapis.com"
  disable_on_destroy = false
}

# Dar permissão ao service agent do Composer
resource "google_project_iam_member" "composer_service_agent" {
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${data.google_project.project.number}@cloudcomposer-accounts.iam.gserviceaccount.com"
  
  depends_on = [google_project_service.composer_api]
}

# Criar o ambiente Composer (Airflow)
resource "google_composer_environment" "data_orchestrator" {
  name    = "data-pipeline-orchestrator"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-2-airflow-2"
      
      # Variáveis de ambiente disponíveis nas DAGs
      env_variables = {
        BUCKET_RAW = "${var.project_id}-raw-data"
      }
    }

    # Configuração SMALL (econômica para labs)
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 1
        storage_gb = 0.5
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 1
        storage_gb = 0.5
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 0.5
        min_count  = 1
        max_count  = 2
      }
    }

    node_config {
      network         = "default"
      subnetwork      = "default"
      service_account = google_service_account.composer_sa.email
    }
  }

  depends_on = [
    google_project_service.composer_api,
    google_project_iam_member.composer_service_agent
  ]
}

# Service Account para o Composer
resource "google_service_account" "composer_sa" {
  account_id   = "composer-orchestrator"
  display_name = "Composer Orchestrator Service Account"
  project      = var.project_id
}

# Permissões necessárias
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Output da URL da UI do Airflow
output "airflow_ui_url" {
  value       = google_composer_environment.data_orchestrator.config[0].airflow_uri
  description = "URL da interface web do Airflow"
}

output "composer_dag_bucket" {
  value       = google_composer_environment.data_orchestrator.config[0].dag_gcs_prefix
  description = "Caminho do bucket onde você deve colocar as DAGs"
}
