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


# -----------------------------------------------------------------------------
# Helpers (Dataform)
# -----------------------------------------------------------------------------
def _last_segment(value: str | None) -> str | None:
    """Returns the last segment of a resource name (after the last '/')."""
    if not value:
        return value
    return value.strip().rstrip("/").split("/")[-1]


def _dataform_repo_id(raw: str | None) -> str | None:
    """
    Accepts:
      - "iot-transformations"
      - "projects/.../locations/.../repositories/iot-transformations"
    Returns always the short repo id.
    """
    return _last_segment(raw)


def _dataform_workspace_name(
    raw: str | None,
    *,
    project_id: str,
    region: str,
    repository_id: str | None,
) -> str | None:
    """
    Dataform workspace must be the FULL resource name for the API.

    Accepts:
      - "pde_gcp" (short id) -> builds the full resource name
      - "projects/.../workspaces/pde_gcp" -> uses as-is
    """
    if not raw:
        return raw

    raw = raw.strip().rstrip("/")

    # Already full resource name
    if raw.startswith("projects/") and "/workspaces/" in raw:
        return raw

    # Short id -> build full name
    ws_id = _last_segment(raw)

    if not repository_id:
        # Can't build without repo id
        return None

    return (
        f"projects/{project_id}/locations/{region}"
        f"/repositories/{repository_id}/workspaces/{ws_id}"
    )


# -----------------------------------------------------------------------------
# Config via Airflow Variables
# -----------------------------------------------------------------------------
PROJECT_ID = Variable.get("gcp_project_id", default_var="quick-cache-484111-j4")

# Composer region (Dataform in this region)
COMPOSER_REGION = Variable.get("gcp_region", default_var="us-central1")

# Dataproc region/zone (can be different from Composer)
DATAPROC_REGION = Variable.get("dataproc_region", default_var="us-east1")
DATAPROC_ZONE = Variable.get("dataproc_zone", default_var=f"{DATAPROC_REGION}-b")

LANDING_BUCKET = Variable.get("landing_bucket")
RAW_BUCKET = Variable.get("raw_bucket")

METADATA_OBJ = Variable.get(
    "landing_metadata_obj",
    default_var="batch_input/metadata/sensors_metadata.csv",
)
HISTORY_OBJ = Variable.get(
    "landing_history_obj",
    default_var="batch_input/history/historical_logs.csv",
)

PYSPARK_URI = Variable.get(
    "dataproc_pyspark_uri",
    default_var=f"gs://{RAW_BUCKET}/jobs/spark_batch_ingest.py",
)

# Dataform
DATAFORM_REPO_ID_RAW = Variable.get("dataform_repo_id")
DATAFORM_WORKSPACE_RAW = Variable.get("dataform_workspace_id")

DATAFORM_REPO_ID = _dataform_repo_id(DATAFORM_REPO_ID_RAW)  # e.g. "iot-transformations"
DATAFORM_WORKSPACE_NAME = _dataform_workspace_name(
    DATAFORM_WORKSPACE_RAW,
    project_id=PROJECT_ID,
    region=COMPOSER_REGION,
    repository_id=DATAFORM_REPO_ID,
)

DATAFORM_TAGS = Variable.get("dataform_tags", default_var="gold,dw")
INCLUDED_TAGS = [t.strip() for t in DATAFORM_TAGS.split(",") if t.strip()]

CLUSTER_NAME = "dp-batch-{{ ds_nodash }}"
DEFAULT_ARGS = {"owner": "data-eng-lab", "retries": 0}


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="03_end_to_end_diagram",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["end-to-end", "batch", "gold", "dw", "dataproc", "dataform"],
) as dag:
    # --- Wait for landing inputs (GCS objects in LANDING_BUCKET)
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

    # --- Ephemeral Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_ephemeral_cluster",
        project_id=PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "gce_cluster_config": {"zone_uri": DATAPROC_ZONE},
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

    # --- Submit PySpark (writes to RAW in readings/v1)
    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_spark_job",
        project_id=PROJECT_ID,
        region=DATAPROC_REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_URI,
                "args": [
                    f"--landing_bucket={LANDING_BUCKET}",
                    f"--raw_bucket={RAW_BUCKET}",
                    f"--metadata_object={METADATA_OBJ}",
                    f"--history_object={HISTORY_OBJ}",
                    "--raw_output_prefix=readings/v1",
                    "--batch_id={{ ts_nodash }}",
                ],
            },
        },
    )

    # --- Delete cluster (IMPORTANT: no ignore_if_missing to keep Composer compatible)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ephemeral_cluster",
        project_id=PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # --- Dataform compile
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="dataform_compile",
        project_id=PROJECT_ID,
        region=COMPOSER_REGION,
        repository_id=DATAFORM_REPO_ID,  # short repo id
        compilation_result={"workspace": DATAFORM_WORKSPACE_NAME},  # full workspace resource name
    )

    # --- Dataform invoke (if no tags, run everything)
    workflow_invocation_payload: dict = {
        "compilation_result": "{{ ti.xcom_pull(task_ids='dataform_compile')['name'] }}"
    }
    if INCLUDED_TAGS:
        workflow_invocation_payload["invocation_config"] = {
            "included_tags": INCLUDED_TAGS,
            "transitive_dependencies_included": True,
        }

    invoke_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="dataform_invoke",
        project_id=PROJECT_ID,
        region=COMPOSER_REGION,
        repository_id=DATAFORM_REPO_ID,
        workflow_invocation=workflow_invocation_payload,
    )

    # --- Dataform wait
    wait_dataform = DataformGetWorkflowInvocationOperator(
        task_id="dataform_wait",
        project_id=PROJECT_ID,
        region=COMPOSER_REGION,
        repository_id=DATAFORM_REPO_ID,
        workflow_invocation_id="{{ ti.xcom_pull(task_ids='dataform_invoke')['name'].split('/')[-1] }}",
    )

    # --- Dependencies
    [wait_metadata, wait_history] >> create_cluster >> submit_pyspark >> delete_cluster
    delete_cluster >> compile_dataform >> invoke_dataform >> wait_dataform
