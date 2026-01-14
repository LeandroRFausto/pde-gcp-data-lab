# --- STORAGE (LANDING ZONE & RAW ZONE) ---
resource "google_storage_bucket" "landing_zone" {
  name          = "${var.project_id}-landing-zone"
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = true
}

# --- PUBSUB (STREAMING) ---
resource "google_pubsub_topic" "iot_topic" {
  name = "iot-readings"
}

resource "google_pubsub_subscription" "iot_subscription" {
  name  = "iot-readings-sub"
  topic = google_pubsub_topic.iot_topic.name
}

# --- BIGQUERY (DATA WAREHOUSE) ---
resource "google_bigquery_dataset" "gold_layer" {
  dataset_id                 = "gold_layer"
  friendly_name              = "Gold Layer (Analytics)"
  description                = "Camada final para Analytics e Dataform"
  location                   = var.region
  delete_contents_on_destroy = true
}

# --- TABELA EXTERNA: HISTÓRICO (BATCH) ---
resource "google_bigquery_table" "history_logs" {
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "history_logs"
  deletion_protection = false 

  external_data_configuration {
    autodetect    = false
    source_format = "CSV"
    source_uris   = ["gs://${google_storage_bucket.landing_zone.name}/batch_input/history/*.csv"]
    
    csv_options {
      quote = "\""
      skip_leading_rows = 1
    }
    
    schema = <<EOF
[
  {
    "name": "sensor_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "temperature",
    "type": "FLOAT64",
    "mode": "NULLABLE"
  }
]
EOF
  }
}

# --- TABELA EXTERNA: METADADOS ---
#resource "google_bigquery_table" "sensors_metadata" {
#  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
#  table_id   = "sensors_metadata"
#  deletion_protection = false

#  external_data_configuration {
#    autodetect    = true 
#    source_format = "CSV"
#    source_uris   = ["gs://${google_storage_bucket.landing_zone.name}/batch_input/metadata/*.csv"]
#    csv_options {
#      quote = "\""
#      skip_leading_rows = 1
#    }
#  }
#}

# --- TABELA NATIVA: STREAMING (IOT) ---
resource "google_bigquery_table" "iot_readings" {
  dataset_id = google_bigquery_dataset.gold_layer.dataset_id
  table_id   = "iot_readings"
  deletion_protection = false

  schema = <<EOF
[
  {
    "name": "sensor_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "temperature",
    "type": "FLOAT64",
    "mode": "NULLABLE"
  },
  {
    "name": "humidity",
    "type": "FLOAT64",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  }
]
EOF
}

# --- DATAPLEX (GOVERNANÇA) ---
resource "google_dataplex_lake" "logistics_lake" {
  name     = "logistics-lake"
  location = var.region
}

resource "google_dataplex_zone" "raw_zone" {
  name         = "raw-zone"
  lake         = google_dataplex_lake.logistics_lake.name
  location     = var.region
  type         = "RAW"
  
  discovery_spec {
    enabled = true
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

resource "google_dataplex_asset" "landing_zone_asset" {
  name          = "landing-zone-asset"
  lake          = google_dataplex_lake.logistics_lake.name
  dataplex_zone = google_dataplex_zone.raw_zone.name
  location      = var.region

  discovery_spec {
    enabled = true
  }

  resource_spec {
    name = "projects/${var.project_id}/buckets/${google_storage_bucket.landing_zone.name}"
    type = "STORAGE_BUCKET"
  }
}

# --- DATAFORM (TRANSFORMAÇÃO) ---
resource "google_dataform_repository" "transformation_repo" {
  provider = google-beta
  name     = "iot-transformations"
  region   = var.region
}






# --- Datasets que faltavam ---

resource "google_bigquery_dataset" "raw_zone" {
  dataset_id  = "raw_zone"
  project     = var.project_id
  location    = var.region
  description = "Camada Raw: Dados brutos (CSV, JSON)"
}

resource "google_bigquery_dataset" "sensor_dw" {
  dataset_id  = "sensor_dw"
  project     = var.project_id
  location    = var.region
  description = "Camada DW: Dados de streaming e tabelas unificadas"
}

# --- Tabela Externa (Agora vai funcionar) ---

resource "google_bigquery_table" "batch_readings" {
  dataset_id = google_bigquery_dataset.raw_zone.dataset_id
  table_id   = "batch_readings"
  project    = var.project_id
  
  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris = [
      "gs://${var.project_id}-raw-data/readings/*.csv"
    ]
  }
}


