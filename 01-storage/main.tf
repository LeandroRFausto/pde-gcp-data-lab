# --- BUCKETS GCS ---

# Landing Zone (Entrada de arquivos) - Limpeza em 1 dia
resource "google_storage_bucket" "landing" {
  name          = "${var.project_id}-landing-zone"
  location      = var.region
  force_destroy = true # Permite destruir bucket com arquivos dentro
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 1 }
    action    { type = "Delete" }
  }
}

# Raw Zone (Processamento) - Limpeza em 3 horas (Diagrama)
resource "google_storage_bucket" "raw" {
  name          = "${var.project_id}-raw-zone"
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 1 } # GCS aceita min de 1 dia na regra simples, usaremos 1 dia para simplificar o TF
    action    { type = "Delete" }
  }
  # Nota: Para 3h exatas, precisariamos de um script externo ou Cloud Function, 
  # mas 1 dia é suficiente para o Free Tier não cobrar quase nada.
}

# --- BIGQUERY ---

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = "gold_layer"
  friendly_name              = "Gold Layer (EDW)"
  description                = "Dados tratados e prontos para consumo"
  location                   = var.region
  delete_contents_on_destroy = true # CUIDADO: Em prod, isso seria false
}

resource "google_bigquery_dataset" "dlq" {
  dataset_id                 = "dlq_layer"
  friendly_name              = "Dead Letter Queue"
  description                = "Dados rejeitados ou inconsistentes"
  location                   = var.region
  delete_contents_on_destroy = true
}

# --- FIRESTORE (NOSQL) ---

resource "google_firestore_database" "database" {
  name                              = "(default)"
  location_id                       = "nam5" # Multi-region US (inclui us-central1)
  type                              = "FIRESTORE_NATIVE"
  concurrency_mode                  = "OPTIMISTIC"
  app_engine_integration_mode       = "DISABLED"
  
  # Firestore as vezes demora para provisionar
  depends_on = [google_bigquery_dataset.gold]
}

# --- DATAPLEX (GOVERNANÇA) ---

resource "google_dataplex_lake" "main_lake" {
  name         = "logistics-lake"
  location     = var.region
  display_name = "Logistics Data Lake"
  
  labels = {
    env = "free-tier-lab"
  }
}

resource "google_dataplex_zone" "raw_zone" {
  name         = "raw-zone"
  location     = var.region
  lake         = google_dataplex_lake.main_lake.name
  type         = "RAW"
  discovery_spec {
    enabled = true
  }

  resource_spec {
    location_type = "SINGLE_REGION"
  }
}

# --- TABELA IOT (Adicionada para o Dataflow) ---

resource "google_bigquery_table" "iot_readings" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "iot_readings"
  
  # Schema JSON igual ao dado gerado pelo Python
  schema = <<JSON
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
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "vibration",
    "type": "FLOAT",
    "mode": "NULLABLE"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "location",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
JSON

  deletion_protection = false # Permite destruir via terraform
}
