# ------------------------------------------------------------
# STORAGE MODULE
# - GCS buckets: landing + raw
# - BigQuery datasets: raw_zone + gold_layer (+ opcionais)
# - BigQuery tables: external (opcional) + gold table
# - Dataplex: lake + zone + asset
# ------------------------------------------------------------

# -------------------
# GCS BUCKETS
# -------------------
resource "google_storage_bucket" "landing" {
  name          = "${var.project_id}-landing-zone"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-raw-zone"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# -------------------
# BIGQUERY DATASETS
# -------------------
# IMPORTANTE:
# - BigQuery dataset location NÃO pode mudar depois de criado.
# - Como seu projeto já tem datasets em "us-central1", mantenha consistente com var.region.
# - Se você quisesse "US" multi-region, teria que criar assim desde o início.

resource "google_bigquery_dataset" "raw_zone" {
  project    = var.project_id
  dataset_id = "raw_zone"
  location   = var.bq_raw_location

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "gold_layer" {
  project    = var.project_id
  dataset_id = "gold_layer"
  location   = var.bq_gold_location

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "sensor_dw" {
  project    = var.project_id
  dataset_id = "sensor_dw"
  location   = var.bq_dw_location

  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "dlq_layer" {
  project    = var.project_id
  dataset_id = "dlq_layer"
  location   = var.bq_dlq_location

  delete_contents_on_destroy = false
}

# -------------------
# BIGQUERY TABLES
# -------------------

# DIM external table (batch) lendo do RAW GCS (Parquet)
resource "google_bigquery_table" "dim_sensors_ext" {
  count      = var.enable_external_tables ? 1 : 0
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw_zone.dataset_id
  table_id   = "dim_sensors_ext"

  deletion_protection = false

  external_data_configuration {
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.raw.name}/dims/dim_sensors/*.parquet"]
    autodetect    = true
  }

  depends_on = [google_storage_bucket.raw]
}

# FACT external table (batch) lendo do RAW GCS em Parquet particionado (event_date=YYYY-MM-DD)
resource "google_bigquery_table" "iot_readings_batch_ext" {
  count      = var.enable_external_tables ? 1 : 0
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw_zone.dataset_id
  table_id   = "iot_readings_batch_ext"

  deletion_protection = false

  external_data_configuration {
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.raw.name}/facts/iot_readings_batch/*/*.parquet"]
    autodetect    = true

    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${google_storage_bucket.raw.name}/facts/iot_readings_batch/"
      require_partition_filter = true
    }
  }

  depends_on = [google_storage_bucket.raw]
}

# Gold table (consumo / destino de streaming ou batch final)
resource "google_bigquery_table" "iot_readings" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "iot_readings"

  deletion_protection = false

  schema = jsonencode([
    { name = "sensor_id", type = "STRING", mode = "NULLABLE" },
    { name = "temperature", type = "FLOAT64", mode = "NULLABLE" },
    { name = "humidity", type = "FLOAT64", mode = "NULLABLE" },
    { name = "timestamp", type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}

# -------------------
# DATAPLEX (Lake + Zone + Asset)
# -------------------

resource "google_project_service" "dataplex_api" {
  project            = var.project_id
  service            = "dataplex.googleapis.com"
  disable_on_destroy = false
}

resource "google_dataplex_lake" "main_lake" {
  project  = var.project_id
  name     = "main-lake"
  location = var.region

  depends_on = [google_project_service.dataplex_api]
}

resource "google_dataplex_zone" "raw_zone" {
  project  = var.project_id
  name     = "raw-zone"
  location = var.region
  lake     = google_dataplex_lake.main_lake.name
  type     = "RAW"

  discovery_spec {
    enabled = false
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }

  depends_on = [google_dataplex_lake.main_lake]
}

resource "google_dataplex_asset" "raw_bucket" {
  project       = var.project_id
  name          = "raw-bucket"
  location      = var.region
  lake          = google_dataplex_lake.main_lake.name
  dataplex_zone = google_dataplex_zone.raw_zone.name

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/buckets/${google_storage_bucket.raw.name}"
    type = "STORAGE_BUCKET"
  }

  depends_on = [
    google_dataplex_zone.raw_zone,
    google_storage_bucket.raw
  ]
}
