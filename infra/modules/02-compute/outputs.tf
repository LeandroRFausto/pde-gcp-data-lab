output "pubsub_topic_name" {
  description = "Nome do topic Pub/Sub."
  value       = google_pubsub_topic.iot_topic.name
}

output "pubsub_subscription_name" {
  description = "Nome da subscription Pub/Sub."
  value       = google_pubsub_subscription.iot_subscription.name
}

output "dataflow_service_account_email" {
  description = "Email da Service Account usada pelo Dataflow."
  value       = google_service_account.dataflow_sa.email
}

output "dataflow_job_name" {
  description = "Nome do job Dataflow (se habilitado)."
  value       = var.enable_dataflow_job ? google_dataflow_job.iot_ingestion[0].name : null
}

output "airflow_ui_url" {
  description = "URL da interface web do Airflow (Composer)."
  value       = var.enable_composer ? google_composer_environment.data_orchestrator[0].config[0].airflow_uri : null
}

output "composer_dag_bucket" {
  description = "Prefixo GCS onde colocar DAGs (Composer)."
  value       = var.enable_composer ? google_composer_environment.data_orchestrator[0].config[0].dag_gcs_prefix : null
}
