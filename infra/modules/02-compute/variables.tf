variable "project_id" {
  type        = string
  description = "ID do projeto GCP."
}

variable "region" {
  type        = string
  description = "Região principal do deployment (Pub/Sub, Dataflow, Composer, etc.)."
}

variable "landing_bucket_name" {
  type        = string
  description = "Nome do bucket de landing (usado como temp_gcs_location no Dataflow)."
}

variable "gold_dataset_id" {
  type        = string
  description = "ID do dataset BigQuery (Gold Layer) usado no outputTableSpec do Dataflow e/ou nas transformações."
}

variable "raw_bucket_name" {
  type        = string
  description = "Nome do bucket RAW (para expor ao Composer via env var BUCKET_RAW)."

  # Opcional: deixe default temporário para não quebrar enquanto você ainda não passou output do módulo storage.
  # Quando o storage module já exportar raw_bucket, remova o default e passe via root main.tf.
  default = null
}

variable "pubsub_topic_name" {
  type        = string
  description = "Nome do Pub/Sub topic."
  default     = "iot-readings"
}

variable "pubsub_subscription_name" {
  type        = string
  description = "Nome da subscription do Pub/Sub."
  default     = "iot-readings-sub"
}

variable "enable_composer" {
  type        = bool
  description = "Habilita Cloud Composer (Airflow)."
  default     = false
}

variable "enable_dataflow_job" {
  type        = bool
  description = "Se true, o Terraform inicia um job Dataflow (execução). Para portfolio, recomendo manter false."
  default     = false
}

variable "dataflow_template_region" {
  type        = string
  description = "Região do bucket de templates do Dataflow (ex: us-central1). Deve bater com o template_gcs_path."
  default     = "us-central1"
}

variable "project_number" {
  type        = string
  description = "Project Number do GCP."
}

