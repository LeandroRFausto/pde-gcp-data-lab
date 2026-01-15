# -------------------
# GCS
# -------------------

resource "google_storage_bucket" "landing_zone" {
  project       = var.project_id
  name          = "${var.project_id}-landing-zone"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  # Truncar landing diariamente
  lifecycle_rule {
    condition { age = 1 }
    action { type = "Delete" }
  }
}

resource "google_storage_bucket" "raw_data" {
  project       = var.project_id
  name          = "${var.project_id}-raw-data"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# Prefixos úteis (opcional)
resource "google_storage_bucket_object" "raw_prefixes" {
  for_each = toset(["readings/", "temp/", "staging/", "deadletter/"])

  bucket  = google_storage_bucket.raw_data.name
  name    = each.value
  content = ""
}

# -------------------
# PUBSUB
# -------------------

resource "google_pubsub_topic" "iot_topic" {
  project = var.project_id
  name    = "iot-readings"
}

resource "google_pubsub_subscription" "iot_subscription" {
  project = var.project_id
  name    = "iot-readings-sub"
  topic   = google_pubsub_topic.iot_topic.name
}

# -------------------
# BIGQUERY DATASETS
# -------------------

resource "google_bigquery_dataset" "raw_zone" {
  project     = var.project_id
  dataset_id  = "raw_zone"
  location    = var.region
  description = "Camada Raw: Dados brutos (CSV, JSON)"
}

resource "google_bigquery_dataset" "gold_layer" {
  project                    = var.project_id
  dataset_id                 = "gold_layer"
  friendly_name              = "Gold Layer (Analytics)"
  description                = "Camada final para Analytics e Dataform"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "sensor_dw" {
  project     = var.project_id
  dataset_id  = "sensor_dw"
  location    = var.region
  description = "Camada DW: fatos/dimensões derivadas da gold"
}

# -------------------
# BIGQUERY TABLES
# -------------------

# External table (batch) lendo do RAW GCS
resource "google_bigquery_table" "batch_readings" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw_zone.dataset_id
  table_id   = "batch_readings"

  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris   = ["gs://${google_storage_bucket.raw_data.name}/readings/*.csv"]
  }

  depends_on = [google_storage_bucket.raw_data]
}

# Gold table (streaming / consumo)
resource "google_bigquery_table" "iot_readings" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "iot_readings"

  deletion_protection = false

  schema = <<EOF
[
  {"name":"sensor_id","type":"STRING","mode":"NULLABLE"},
  {"name":"temperature","type":"FLOAT64","mode":"NULLABLE"},
  {"name":"humidity","type":"FLOAT64","mode":"NULLABLE"},
  {"name":"timestamp","type":"TIMESTAMP","mode":"NULLABLE"}
]
EOF
}

# (Opcional) External history_logs na landing — só se você realmente usa esse path
resource "google_bigquery_table" "history_logs" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "history_logs"

  deletion_protection = false

  external_data_configuration {
    autodetect    = false
    source_format = "CSV"
    source_uris   = ["gs://${google_storage_bucket.landing_zone.name}/batch_input/history/*.csv"]

    csv_options {
      quote             = "\""
      skip_leading_rows = 1
    }

    schema = <<EOF
[
  {"name":"sensor_id","type":"STRING","mode":"NULLABLE"},
  {"name":"timestamp","type":"TIMESTAMP","mode":"NULLABLE"},
  {"name":"temperature","type":"FLOAT64","mode":"NULLABLE"}
]
EOF
  }

  depends_on = [google_storage_bucket.landing_zone]
}

# -------------------
# DATAPLEX
# -------------------

resource "google_dataplex_lake" "logistics_lake" {
  project  = var.project_id
  name     = "logistics-lake"
  location = var.region
}

resource "google_dataplex_zone" "raw_zone" {
  project  = var.project_id
  name     = "raw-zone"
  lake     = google_dataplex_lake.logistics_lake.name
  location = var.region
  type     = "RAW"

  discovery_spec { enabled = true }

  resource_spec { location_type = "SINGLE_REGION" }
}

resource "google_dataplex_asset" "landing_zone_asset" {
  project       = var.project_id
  name          = "landing-zone-asset"
  lake          = google_dataplex_lake.logistics_lake.name
  dataplex_zone = google_dataplex_zone.raw_zone.name
  location      = var.region

  discovery_spec { enabled = true }

  resource_spec {
    name = "projects/${var.project_id}/buckets/${google_storage_bucket.landing_zone.name}"
    type = "STORAGE_BUCKET"
  }

  depends_on = [google_storage_bucket.landing_zone, google_dataplex_zone.raw_zone]
}

# -------------------
# DATAFORM (repo only)
# -------------------

resource "google_dataform_repository" "transformation_repo" {
  provider = google-beta
  project  = var.project_id
  region   = var.region
  name     = "iot-transformations"

  depends_on = [google_project_service.dataform_api]
}
