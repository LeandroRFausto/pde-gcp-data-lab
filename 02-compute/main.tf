# --- PUBSUB ---
resource "google_pubsub_topic" "iot_topic" {
  name = "iot-telemetry-topic"
}
resource "google_pubsub_subscription" "iot_sub" {
  name  = "iot-telemetry-sub"
  topic = google_pubsub_topic.iot_topic.name
  message_retention_duration = "600s"
}

# --- IAM ---
resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-runner-sa"
  display_name = "Dataflow Service Account"
}

resource "google_project_iam_member" "dataflow_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.editor",    # <--- AQUI ESTÁ A MUDANÇA (Substitui subscriber/viewer)
    "roles/storage.objectAdmin"
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# --- JOB ---
resource "google_dataflow_job" "iot_ingestion" {
  name              = "iot-to-bq-job"
  template_gcs_path = "gs://dataflow-templates-us-central1/latest/PubSub_to_BigQuery"
  temp_gcs_location = "gs://${var.project_id}-landing-zone/temp"
  service_account_email = google_service_account.dataflow_sa.email
  
  region            = var.region
  zone              = "us-central1-a"
  
  parameters = {
    inputTopic      = google_pubsub_topic.iot_topic.id
    outputTableSpec = "${var.project_id}:gold_layer.iot_readings"
  }

  on_delete = "cancel"
}
