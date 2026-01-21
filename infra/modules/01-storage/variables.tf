variable "project_id" {
  type        = string
  description = "ID do projeto GCP."
}

variable "region" {
  type        = string
  description = "Regiao principal onde os recursos regionais serao criados."
}

variable "enable_firestore" {
  type        = bool
  description = "Cria Firestore default database."
  default     = false
}

variable "firestore_location_id" {
  type        = string
  description = "Location do Firestore (ex: nam5)."
  default     = "nam5"
}

variable "firestore_deletion_policy" {
  type        = string
  description = "ABANDON (nao deleta no destroy) ou DELETE."
  default     = "ABANDON"

  validation {
    condition     = contains(["ABANDON", "DELETE"], var.firestore_deletion_policy)
    error_message = "firestore_deletion_policy deve ser ABANDON ou DELETE."
  }
}

variable "enable_external_tables" {
  type        = bool
  description = "Se true, cria external tables no BigQuery apontando para Parquet no RAW GCS. Mantenha false no primeiro apply."
  default     = false
}

variable "enable_dataflow_job" {
  type        = bool
  description = "Se true, inicia job Dataflow no apply. Para portfolio, manter false."
  default     = false
}

variable "dataflow_template_region" {
  type        = string
  description = "Região do bucket de templates do Dataflow."
  default     = "us-central1"
}

variable "bq_raw_location" {
  type        = string
  description = "Location do dataset raw_zone."
  default     = "us-central1"
}

variable "bq_gold_location" {
  type        = string
  description = "Location do dataset gold_layer."
  default     = "US"
}

variable "bq_dw_location" {
  type        = string
  description = "Location do dataset sensor_dw."
  default     = "US"
}

variable "bq_dlq_location" {
  type        = string
  description = "Location do dataset dlq_layer."
  default     = "US"
}

