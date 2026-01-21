from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# Config
# =========================
PROJECT_ID = Variable.get("gcp_project_id", default_var="quick-cache-484111-j4")

BQ_LOCATION = "US"

RAW_DATASET = Variable.get("bq_raw_dataset", default_var="raw_zone")
GOLD_DATASET = Variable.get("bq_gold_dataset", default_var="gold_layer")

RAW_FACT_EXT = Variable.get(
    "bq_raw_fact_ext",
    default_var="iot_readings_batch_ext",
)

GOLD_TABLE = Variable.get(
    "bq_gold_table",
    default_var="iot_readings_gold",
)

# Janela de leitura (obrigatória por require_partition_filter)
LOOKBACK_DAYS = int(
    Variable.get("gold_lookback_days", default_var="30")
)

DEFAULT_ARGS = {"owner": "data-eng-lab", "retries": 0}

with DAG(
    dag_id="batch_raw_to_gold",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["batch", "raw", "gold", "bigquery"],
) as dag:

    transform_raw_to_gold = BigQueryInsertJobOperator(
        task_id="transform_raw_to_gold",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "useLegacySql": False,
                "query": f"""
DECLARE cutoff_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY);

CREATE OR REPLACE TABLE `{PROJECT_ID}.{GOLD_DATASET}.{GOLD_TABLE}` AS
SELECT
  sensor_id,
  timestamp,
  temperature,
  ingestion_time,
  batch_id,
  source
FROM `{PROJECT_ID}.{RAW_DATASET}.{RAW_FACT_EXT}`
WHERE event_date >= cutoff_date;
""",
            }
        },
    )

    transform_raw_to_gold
