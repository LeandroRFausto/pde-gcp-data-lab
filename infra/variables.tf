variable "project_id" {
  type        = string
  description = "ID do projeto GCP."
}

variable "region" {
  type        = string
  description = "Região principal onde os recursos regionais serão criados."
}

# ------------------------------------------------------------
# Firestore (opcional)
# ------------------------------------------------------------
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
  description = "ABANDON (não deleta no destroy) ou DELETE."
  default     = "ABANDON"

  validation {
    condition     = contains(["ABANDON", "DELETE"], var.firestore_deletion_policy)
    error_message = "firestore_deletion_policy deve ser ABANDON ou DELETE."
  }
}

# ------------------------------------------------------------
# BigQuery External Tables
# ------------------------------------------------------------
variable "enable_external_tables" {
  type        = bool
  description = "Cria external tables no BigQuery apontando para Parquet no RAW GCS. Ligue depois do DAG gerar os arquivos."
  default     = false
}

# ------------------------------------------------------------
# Dataflow (execução opcional)
# ------------------------------------------------------------
variable "enable_dataflow_job" {
  type        = bool
  description = "Se true, inicia job Dataflow no terraform apply. Para portfolio, manter false."
  default     = false
}

variable "dataflow_template_region" {
  type        = string
  description = "Região do bucket de templates do Dataflow."
  default     = "us-central1"
}

# ------------------------------------------------------------
# Composer (Airflow)
# ------------------------------------------------------------
variable "enable_composer" {
  type        = bool
  description = "Habilita Cloud Composer (Airflow)."
  default     = false
}

# ------------------------------------------------------------
# Governance / Dataform (opcional)
# ------------------------------------------------------------
variable "enable_dataform" {
  type        = bool
  description = "Habilita recursos de Dataform."
  default     = false
}

variable "project_number" {
  type        = string
  description = "Project Number do GCP (necessário para IAM do Composer service agent)."
}

variable "access_token" {
  type        = string
  description = "Token temporário do gcloud para autenticação (evita metadata EOF no Cloud Shell)."
  sensitive   = true
}


