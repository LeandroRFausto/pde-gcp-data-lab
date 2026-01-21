from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = Variable.get("gcp_project_id")
REGION = Variable.get("gcp_region", default_var="us-central1")
ZONE = Variable.get("gcp_zone", default_var=f"{REGION}-b")

LANDING_BUCKET = Variable.get("landing_bucket")   # ex: <project>-landing-zone
RAW_BUCKET = Variable.get("raw_bucket")           # ex: <project>-raw-zone

METADATA_OBJ = "batch_input/metadata/sensors_metadata.csv"
HISTORY_OBJ = "batch_input/history/historical_logs.csv"

PYSPARK_URI = Variable.get(
    "dataproc_pyspark_uri",
    default_var=f"gs://{RAW_BUCKET}/jobs/spark_batch_ingest.py",
)

# Nome simples, sempre válido e curto
CLUSTER_NAME = "dp-batch-{{ ds_nodash }}"

DEFAULT_ARGS = {"owner": "data-eng-lab", "retries": 0}

with DAG(
    dag_id="02_dataproc_batch_to_raw",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["batch", "dataproc", "raw"],
) as dag:

    wait_metadata = GCSObjectExistenceSensor(
        task_id="wait_metadata_csv",
        bucket=LANDING_BUCKET,
        object=METADATA_OBJ,
        poke_interval=30,
        timeout=10 * 60,
    )

    wait_history = GCSObjectExistenceSensor(
        task_id="wait_history_csv",
        bucket=LANDING_BUCKET,
        object=HISTORY_OBJ,
        poke_interval=30,
        timeout=10 * 60,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_ephemeral_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "gce_cluster_config": {
                "zone_uri": ZONE,
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 30},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 30},
            },
            "software_config": {
                "image_version": "2.1-debian12",
                "properties": {"spark:spark.sql.session.timeZone": "UTC"},
            },
            "lifecycle_config": {
                "auto_delete_ttl": {"seconds": 60 * 60}
            },
        },
    )

    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
                "args": [
                    f"--landing_bucket={LANDING_BUCKET}",
                    f"--raw_bucket={RAW_BUCKET}",
                    f"--metadata_object={METADATA_OBJ}",
                    f"--history_object={HISTORY_OBJ}",
                    "--batch_id={{ ts_nodash }}",
                ],
            },
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ephemeral_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [wait_metadata, wait_history] >> create_cluster >> submit_pyspark >> delete_cluster
