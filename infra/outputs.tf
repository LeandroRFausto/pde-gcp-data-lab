output "landing_bucket" {
  description = "Nome do bucket de landing zone."
  value       = module.storage.landing_bucket
}

output "raw_bucket" {
  description = "Nome do bucket de raw zone."
  value       = module.storage.raw_bucket
}

output "gold_dataset_id" {
  description = "Dataset do BigQuery para Gold Layer."
  value       = module.storage.gold_dataset_id
}

output "pubsub_topic" {
  description = "Nome do topic Pub/Sub usado para IoT readings."
  value       = module.compute.pubsub_topic_name
}

output "pubsub_subscription" {
  description = "Nome da subscription Pub/Sub usada para IoT readings."
  value       = module.compute.pubsub_subscription_name
}

output "dataflow_service_account" {
  description = "Service Account usada pelo Dataflow."
  value       = module.compute.dataflow_service_account_email
}

output "dataflow_job_name" {
  description = "Nome do job Dataflow."
  value       = module.compute.dataflow_job_name
}

output "dataform_repository_name" {
  description = "Nome do repositorio Dataform (se habilitado)."
  value       = module.governance.dataform_repository_name
}

output "composer_airflow_ui_url" {
  description = "URL do Airflow no Composer (se habilitado)."
  value       = module.compute.airflow_ui_url
}

output "composer_dag_bucket" {
  description = "Bucket/prefixo de DAGs do Composer (se habilitado)."
  value       = module.compute.composer_dag_bucket
}
