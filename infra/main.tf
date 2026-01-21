# ------------------------------------------------------------
# ROOT: Orquestrador (sem recursos duplicados)
# ------------------------------------------------------------

module "storage" {
  source     = "./modules/01-storage"
  project_id = var.project_id
  region     = var.region

  enable_firestore          = var.enable_firestore
  firestore_location_id     = var.firestore_location_id
  firestore_deletion_policy = var.firestore_deletion_policy

  enable_external_tables = var.enable_external_tables

  # BigQuery dataset locations (para NÃO forçar replace)
  bq_raw_location  = var.region
  bq_gold_location = "US"
  bq_dw_location   = "US"
  bq_dlq_location  = "US"
}

module "governance" {
  source     = "./modules/03-governance"
  project_id = var.project_id
  region     = var.region

  enable_dataform = var.enable_dataform
}

module "compute" {
  source         = "./modules/02-compute"
  project_id     = var.project_id
  project_number = var.project_number
  region         = var.region

  # Outputs vindos do storage
  landing_bucket_name = module.storage.landing_bucket
  raw_bucket_name     = module.storage.raw_bucket
  gold_dataset_id     = module.storage.gold_dataset_id

  # Pub/Sub
  pubsub_topic_name        = "iot-readings"
  pubsub_subscription_name = "iot-readings-sub"

  # Toggles
  enable_composer          = var.enable_composer
  enable_dataflow_job      = var.enable_dataflow_job
  dataflow_template_region = var.dataflow_template_region
}
