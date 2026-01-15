# ------------------------------------------------------------
# Storage module: GCS (landing/raw) + BigQuery (gold/dlq) + Firestore
# + Dataplex lake/zone + (opcional) tabela gold_layer.iot_readings
# ------------------------------------------------------------

# -------------------
# GCS BUCKETS
# -------------------

resource "google_storage_bucket" "landing" {
  project       = var.project_id
  name          = "${var.project_id}-landing-zone"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  # Truncar landing diariamente (GCS lifecycle é por DIA)
  lifecycle_rule {
    condition { age = 1 }
    action    { type = "Delete" }
  }
}

resource "google_storage_bucket" "raw" {
  project       = var.project_id
  # Use o mesmo nome que você já usa no pipeline (temp/staging/readings)
  name          = "${var.project_id}-raw-data"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  # Mantém simples: 1 dia (GCS lifecycle simples trabalha por dia)
  lifecycle_rule {
    condition { age = 1 }
    action    { type = "Delete" }
  }
}

# (Opcional) cria "pastas" padrão no GCS (na prática são objetos vazios)
# Útil para já ter /readings /temp /staging /deadletter visíveis
resource "google_storage_bucket_object" "raw_prefixes" {
  for_each = toset([
    "readings/",
    "temp/",
    "staging/",
    "deadletter/"
  ])

  bucket  = google_storage_bucket.raw.name
  name    = each.value
  content = ""
}

# -------------------
# BIGQUERY DATASETS
# -------------------

resource "google_bigquery_dataset" "gold" {
  project                    = var.project_id
  dataset_id                 = "gold_layer"
  friendly_name              = "Gold Layer (EDW)"
  description                = "Dados tratados e prontos para consumo"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "dlq" {
  project                    = var.project_id
  dataset_id                 = "dlq_layer"
  friendly_name              = "Dead Letter Queue"
  description                = "Dados rejeitados ou inconsistentes"
  location                   = var.region
  delete_contents_on_destroy = true
}

# -------------------
# FIRESTORE
# -------------------
# Importante: Firestore "location_id" não é igual a var.region.
# Você escolheu nam5 (multi-region). Pode manter.
resource "google_firestore_database" "database" {
  project                        = var.project_id
  name                           = "(default)"
  location_id                    = "nam5"
  type                           = "FIRESTORE_NATIVE"
  concurrency_mode               = "OPTIMISTIC"
  app_engine_integration_mode    = "DISABLED"

  # Firestore às vezes demora; depende do projeto pronto, mas não precisa do BQ.
  depends_on = [google_bigquery_dataset.gold]
}

# -------------------
# DATAPLEX
# -------------------

resource "google_dataplex_lake" "main_lake" {
  project      = var.project_id
  name         = "logistics-lake"
  location     = var.region
  display_name = "Logistics Data Lake"

  labels = {
    env = "free-tier-lab"
  }
}

resource "google_dataplex_zone" "raw_zone" {
  project  = var.project_id
  name     = "raw-zone"
  location = var.region
  lake     = google_dataplex_lake.main_lake.name
  type     = "RAW"

  discovery_spec {
    enabled = true
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

# -------------------
# GOLD TABLE (opcional)
# -------------------
# Se você prefere que o Dataflow/queries criem a tabela, pode REMOVER este resource.
# Mas para "infra sempre montada", faz sentido deixar.
resource "google_bigquery_table" "iot_readings" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "iot_readings"

  schema = <<JSON
[
  {"name":"sensor_id","type":"STRING","mode":"NULLABLE"},
  {"name":"temperature","type":"FLOAT","mode":"NULLABLE"},
  {"name":"humidity","type":"FLOAT","mode":"NULLABLE"},
  {"name":"timestamp","type":"TIMESTAMP","mode":"NULLABLE"}
]
JSON

  deletion_protection = false
}

# -------------------
# OUTPUTS
# -------------------

output "landing_bucket" {
  value = google_storage_bucket.landing.name
}

output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "gold_dataset" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "dlq_dataset" {
  value = google_bigquery_dataset.dlq.dataset_id
}
