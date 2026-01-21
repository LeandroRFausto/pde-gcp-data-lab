# ------------------------------------------------------------
# COMPUTE MODULE
# - Pub/Sub
# - Service Accounts + IAM (Dataflow + Composer)
# - (Opcional) Dataflow job (template PubSub_to_BigQuery)
# - (Opcional) Cloud Composer (Airflow)
# ------------------------------------------------------------

# ------------------------------------------------------------
# APIs (habilitar antes do restante)
# ------------------------------------------------------------
locals {
  required_services = [
    "serviceusage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "dataproc.googleapis.com",
    "composer.googleapis.com",
  ]
}

resource "google_project_service" "required" {
  for_each           = toset(local.required_services)
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# ------------------------------------------------------------
# PUB/SUB
# ------------------------------------------------------------
resource "google_pubsub_topic" "iot_topic" {
  project = var.project_id
  name    = var.pubsub_topic_name

  depends_on = [google_project_service.required]
}

resource "google_pubsub_subscription" "iot_subscription" {
  project                    = var.project_id
  name                       = var.pubsub_subscription_name
  topic                      = google_pubsub_topic.iot_topic.name
  message_retention_duration = "600s"

  depends_on = [google_project_service.required]
}

# ------------------------------------------------------------
# DATAFLOW - Service Account + IAM
# ------------------------------------------------------------
resource "google_service_account" "dataflow_sa" {
  project      = var.project_id
  account_id   = "dataflow-runner-sa"
  display_name = "Dataflow Service Account"

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "dataflow_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.editor",
    "roles/storage.objectAdmin",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"

  depends_on = [google_project_service.required]
}

# ------------------------------------------------------------
# DATAFLOW JOB (Opcional)
# Observação: job é "execução". Para portfolio, mantenha desligado (enable_dataflow_job=false)
# ------------------------------------------------------------
resource "google_dataflow_job" "iot_ingestion" {
  count                 = var.enable_dataflow_job ? 1 : 0
  project               = var.project_id
  name                  = "iot-to-bq-job"
  region                = var.region
  template_gcs_path     = "gs://dataflow-templates-${var.dataflow_template_region}/latest/PubSub_to_BigQuery"
  temp_gcs_location     = "gs://${var.landing_bucket_name}/temp"
  service_account_email = google_service_account.dataflow_sa.email

  parameters = {
    inputTopic      = google_pubsub_topic.iot_topic.id
    outputTableSpec = "${var.project_id}:${var.gold_dataset_id}.iot_readings"
  }

  on_delete = "cancel"

  depends_on = [
    google_project_service.required,
    google_project_iam_member.dataflow_roles,
    google_pubsub_topic.iot_topic,
    google_pubsub_subscription.iot_subscription,
  ]
}

# ------------------------------------------------------------
# COMPOSER (Opcional)
# ------------------------------------------------------------
resource "google_service_account" "composer_sa" {
  count        = var.enable_composer ? 1 : 0
  project      = var.project_id
  account_id   = "composer-orchestrator"
  display_name = "Composer Orchestrator Service Account"

  depends_on = [google_project_service.required]
}

# Service Agent role (necessário para Composer 2)
# IMPORTANT: usa project_number vindo do root (evita data.google_project e problemas de metadata no Cloud Shell)
resource "google_project_iam_member" "composer_service_agent" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/composer.ServiceAgentV2Ext"
  member  = "serviceAccount:service-${var.project_number}@cloudcomposer-accounts.iam.gserviceaccount.com"

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "composer_worker" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa[0].email}"

  depends_on = [google_project_service.required]
}

# Para portfolio, pode manter bigquery.admin (simples e funciona).
resource "google_project_iam_member" "composer_bigquery" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.composer_sa[0].email}"

  depends_on = [google_project_service.required]
}

resource "google_project_iam_member" "composer_storage" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.composer_sa[0].email}"

  depends_on = [google_project_service.required]
}

# Permissão para o Composer orquestrar Dataproc efêmero via DAG
resource "google_project_iam_member" "composer_dataproc" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.composer_sa[0].email}"

  depends_on = [google_project_service.required]
}

resource "google_composer_environment" "data_orchestrator" {
  count   = var.enable_composer ? 1 : 0
  name    = "data-pipeline-orchestrator"
  region  = var.region
  project = var.project_id

  config {
    software_config {
      image_version = "composer-2-airflow-2"

      env_variables = {
        BUCKET_RAW = var.raw_bucket_name
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
      network    = "projects/${var.project_id}/global/networks/default"
      subnetwork = "projects/${var.project_id}/regions/${var.region}/subnetworks/default"

      service_account = google_service_account.composer_sa[0].email
    }
  }

  depends_on = [
    google_project_service.required,
    google_project_iam_member.composer_service_agent,
    google_project_iam_member.composer_worker,
    google_project_iam_member.composer_bigquery,
    google_project_iam_member.composer_storage,
    google_project_iam_member.composer_dataproc,
  ]
}
