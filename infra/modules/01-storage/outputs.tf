output "landing_bucket" {
  description = "Bucket da Landing Zone."
  value       = google_storage_bucket.landing.name
}

output "raw_bucket" {
  description = "Bucket da Raw Zone."
  value       = google_storage_bucket.raw.name
}

output "gold_dataset_id" {
  description = "Dataset ID do BigQuery para Gold Layer."
  value       = google_bigquery_dataset.gold_layer.dataset_id
}

output "raw_dataset_id" {
  description = "Dataset ID do BigQuery para Raw Zone."
  value       = google_bigquery_dataset.raw_zone.dataset_id
}

output "dw_dataset_id" {
  description = "Dataset ID do BigQuery para DW (sensor_dw)."
  value       = google_bigquery_dataset.sensor_dw.dataset_id
}

output "dlq_dataset_id" {
  description = "Dataset ID do BigQuery para DLQ."
  value       = google_bigquery_dataset.dlq_layer.dataset_id
}

output "dataplex_lake_name" {
  description = "Nome do Dataplex Lake."
  value       = google_dataplex_lake.main_lake.name
}

output "dataplex_zone_name" {
  description = "Nome da Dataplex Zone RAW."
  value       = google_dataplex_zone.raw_zone.name
}

