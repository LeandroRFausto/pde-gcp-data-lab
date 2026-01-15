from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

PROJECT_ID = "quick-cache-484111-j4"
REGION = "us-central1"
ZONE = "us-central1-b"
VM_NAME = "airflow-vm"

# ---- Ajuste aqui para seu lab ----
BUCKET = f"{PROJECT_ID}-landing-zone"   # ou seu raw bucket
SOURCE_OBJECTS = ["dados_teste.csv"]    # caminho no bucket
RAW_DATASET = "raw_layer"
RAW_TABLE = "iot_readings_raw"

GOLD_DATASET = "gold_layer"
GOLD_TABLE = "iot_readings_gold"

with DAG(
    dag_id="00_end_to_end_stream_batch_transform",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["e2e", "raw", "gold"],
) as dag:

    # 1) "Streaming": garantir que algum processo/serviço está rodando na VM
    # Exemplo: se você tiver um container que lê Pub/Sub e grava no BQ,
    # você pode subir aqui. Idempotente: se já estiver rodando, não faz nada.
    ensure_streaming_running = BashOperator(
        task_id="ensure_streaming_running",
        bash_command=rf"""
set -e
gcloud compute ssh {VM_NAME} \
  --project={PROJECT_ID} \
  --zone={ZONE} \
  --command "sudo bash -lc '
    # exemplo idempotente: sobe um container chamado pubsub_to_bq se existir compose
    if [ -f /opt/airflow/docker-compose.yml ]; then
      cd /opt/airflow
      sudo docker compose ps | grep -q pubsub_to_bq && echo streaming_ok && exit 0
      echo starting_streaming || true
      # se existir esse serviço no compose, sobe; se não existir, só segue
      sudo docker compose up -d pubsub_to_bq || true
    else
      echo no_compose_found
    fi
  '"
""",
    )

    # 2) Batch: GCS -> BigQuery RAW
    load_gcs_to_bq_raw = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq_raw",
        bucket=BUCKET,
        source_objects=SOURCE_OBJECTS,
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_APPEND",  # troque pra WRITE_TRUNCATE se quiser "zerar"
    )

    # 3) Transform: RAW -> GOLD (exemplo simples)
    # Aqui você pode substituir por Dataform depois.
    transform_raw_to_gold = BigQueryInsertJobOperator(
        task_id="transform_raw_to_gold",
        configuration={
            "query": {
                "query": f"""
CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{GOLD_DATASET}`;

CREATE OR REPLACE TABLE `{PROJECT_ID}.{GOLD_DATASET}.{GOLD_TABLE}` AS
SELECT
  *
FROM `{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`;
""",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    ensure_streaming_running >> load_gcs_to_bq_raw >> transform_raw_to_gold
