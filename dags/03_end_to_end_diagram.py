from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetWorkflowInvocationOperator,
)

# =========================
# Config via Airflow Variables
# =========================
PROJECT_ID = Variable.get("gcp_project_id", default_var="quick-cache-484111-j4")
REGION = Variable.get("gcp_region", default_var="us-central1")
ZONE = Variable.get("gcp_zone", default_var=f"{REGION}-b")

LANDING_BUCKET = Variable.get("landing_bucket")  # ex: <project>-landing-zone
RAW_BUCKET = Variable.get("raw_bucket")          # ex: <project>-raw-zone

# Arquivos de entrada batch no landing
METADATA_OBJ = Variable.get(
    "landing_metadata_obj",
    default_var="batch_input/metadata/sensors_metadata.csv",
)
HISTORY_OBJ = Variable.get(
    "landing_history_obj",
    default_var="batch_input/history/historical_logs.csv",
)

# Script Spark (no raw bucket)
PYSPARK_URI = Variable.get(
    "dataproc_pyspark_uri",
    default_var=f"gs://{RAW_BUCKET}/jobs/spark_batch_ingest.py",
)

# Dataform (workspace)
DATAFORM_REPO_ID = Variable.get("dataform_repo_id")           # ex: "iot-analytics"
DATAFORM_WORKSPACE_ID = Variable.get("dataform_workspace_id") # ex: "dev"

# Tags no Dataform (recomendado: marque seus SQLX com tags)
# Ex: gold_layer.iot_readings => tags: ["gold"]
# Ex: sensor_dw.*            => tags: ["dw"]
DATAFORM_TAGS = Variable.get("dataform_tags", default_var="gold,dw")
INCLUDED_TAGS = [t.strip() for t in DATAFORM_TAGS.split(",") if t.strip()]

# Cluster efêmero
CLUSTER_NAME = "dp-batch-{{ ds_nodash }}"

DEFAULT_ARGS = {"owner": "data-eng-lab", "retries": 0}

with DAG(
    dag_id="03_end_to_end_diagram",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,   # manual por enquanto
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["end-to-end", "batch", "gold", "dw", "dataproc", "dataform"],
) as dag:

    # 1) Espera os arquivos chegarem no landing
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

    # 2) Cria cluster Dataproc efêmero
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_ephemeral_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "gce_cluster_config": {"zone_uri": ZONE},
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
            },
            "software_config": {
                "image_version": "2.2-debian12",
                "properties": {"spark:spark.sql.session.timeZone": "UTC"},
            },
            "lifecycle_config": {"auto_delete_ttl": {"seconds": 60 * 60}},
        },
    )

    # 3) Roda Spark: landing -> raw(parquet)
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

    # 4) Deleta cluster sempre (mesmo se falhar)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ephemeral_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 5) Dataform: compila
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="dataform_compile",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPO_ID,
        compilation_result={"workspace": DATAFORM_WORKSPACE_ID},
    )

    # 6) Dataform: roda workflow (gold + dw) filtrando por tags
    invoke_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="dataform_invoke",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPO_ID,
        workflow_invocation={
            "compilation_result": "{{ ti.xcom_pull(task_ids='dataform_compile')['name'] }}",
            "invocation_config": {
                "included_tags": INCLUDED_TAGS
            },
        },
    )

    # 7) Espera terminar
    wait_dataform = DataformGetWorkflowInvocationOperator(
        task_id="dataform_wait",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=DATAFORM_REPO_ID,
        workflow_invocation_id="{{ ti.xcom_pull(task_ids='dataform_invoke')['name'].split('/')[-1] }}",
    )

    # Orquestração conforme diagrama:
    # landing -> dataproc -> raw -> dataform(gold+dw)
    [wait_metadata, wait_history] >> create_cluster >> submit_pyspark >> delete_cluster
    delete_cluster >> compile_dataform >> invoke_dataform >> wait_dataform
