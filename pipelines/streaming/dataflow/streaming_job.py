#!/usr/bin/env python3
"""
streaming_job.py

Dataflow (Apache Beam) streaming pipeline (Flex Template friendly):
- Input: Pub/Sub topic (bytes JSON)
- Valid messages: Write to BigQuery (append, create if needed)
- Optional: write latest state to Firestore (per sensor_id)
- Invalid messages (DLQ):
    - Always write to BigQuery dlq table (append, create if needed)
    - Optional: also write to GCS as windowed files (to avoid GroupByKey-on-global-window error)

IMPORTANT:
- Force BigQuery sink to STREAMING_INSERTS to avoid GroupByKey in streaming.
- For GCS DLQ in streaming, we MUST window the DLQ PCollection before file sinks (WriteToText),
  otherwise Beam raises:
    "GroupByKey cannot be applied to an unbounded PCollection with global windowing and a default trigger".
"""

import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly
from apache_beam.transforms.window import FixedWindows


# -----------------------
# Helpers
# -----------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def str2bool(v: Any) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "on")


def normalize_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, str):
        v = value.strip()
        if "T" not in v and " " in v:
            v = v.replace(" ", "T")
        if v.endswith("Z") or "+" in v:
            return v
        return v + "Z"

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    return None


# -----------------------
# Beam DoFns
# -----------------------
class ParseAndValidate(beam.DoFn):
    DEADLETTER_TAG = "deadletter"

    def process(self, element: bytes):
        raw_str = element.decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw_str)

            if not isinstance(payload, dict):
                raise ValueError("JSON não é objeto/dict")

            sensor_id = payload.get("sensor_id")
            if not sensor_id:
                raise ValueError("Campo obrigatório ausente: sensor_id")

            temp = payload.get("temperature")
            if temp is not None:
                try:
                    payload["temperature"] = float(temp)
                except Exception:
                    raise ValueError(f"temperature inválida: {temp}")

            hum = payload.get("humidity")
            if hum is not None:
                try:
                    payload["humidity"] = float(hum)
                except Exception:
                    raise ValueError(f"humidity inválida: {hum}")

            payload.setdefault("source_type", "streaming")

            ts = normalize_timestamp(payload.get("timestamp"))
            payload["timestamp"] = ts if ts else utc_now_iso()

            payload["processed_at"] = utc_now_iso()

            yield payload

        except Exception as e:
            dlq_row = {
                "raw": raw_str,
                "error": str(e),
                "ingested_at": utc_now_iso(),
            }
            yield pvalue.TaggedOutput(self.DEADLETTER_TAG, dlq_row)


class WriteStateToFirestore(beam.DoFn):
    """
    Firestore writer that won't crash the whole job if Firestore isn't available.
    (Import and client creation happen inside the worker.)
    """

    def __init__(self, project_id: str, collection: str):
        self.project_id = project_id
        self.collection = collection
        self.db = None

    def setup(self):
        try:
            from google.cloud import firestore as fs  # import inside worker
            self.db = fs.Client(project=self.project_id)
        except Exception as e:
            logging.error("Firestore unavailable (import/client failed): %s", e)
            self.db = None

    def process(self, element: Dict[str, Any]):
        if self.db is None:
            return

        sensor_id = str(element.get("sensor_id", "unknown"))
        try:
            doc_ref = self.db.collection(self.collection).document(sensor_id)
            doc_ref.set(
                {
                    "sensor_id": sensor_id,
                    "last_update": utc_now_iso(),
                    "payload": element,
                }
            )
        except Exception as e:
            logging.error("Erro ao escrever no Firestore sensor_id=%s: %s", sensor_id, e)

        yield element


# -----------------------
# Main
# -----------------------
def run(argv=None):
    parser = argparse.ArgumentParser()

    # Script args (template parameters)
    parser.add_argument("--input_topic", required=True, help="projects/<project>/topics/<topic>")
    parser.add_argument("--output_table", required=True, help="project:dataset.table")
    parser.add_argument("--dlq_table", required=True, help="project:dataset.table (dlq_layer.xxx)")
    parser.add_argument("--project_id", required=True, help="Project ID (usado no Firestore client)")

    # Optional GCS DLQ (windowed file sink)
    parser.add_argument(
        "--deadletter_path",
        default="",
        help=(
            "GCS prefix for DLQ files, e.g. gs://<bucket>/dataflow/dlq/iot_readings_dlq. "
            "If empty, DLQ-to-GCS is disabled."
        ),
    )
    parser.add_argument(
        "--deadletter_window_sec",
        type=int,
        default=60,
        help="Fixed window size (seconds) for DLQ-to-GCS. Default=60.",
    )
    parser.add_argument(
        "--deadletter_trigger_sec",
        type=int,
        default=30,
        help="Processing-time trigger (seconds) for emitting DLQ files per window. Default=30.",
    )

    # Dataflow-required args
    parser.add_argument("--project", required=True, help="GCP project para o Dataflow")
    parser.add_argument("--region", required=True, help="Região do Dataflow")

    # Firestore controls (template-friendly: true/false)
    parser.add_argument("--enable_firestore", default="true", help="true/false. Default=false")
    parser.add_argument("--firestore_collection", default="sensors", help="Coleção Firestore")

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Ensure project & region are present in PipelineOptions
    pipeline_args = list(pipeline_args) + ["--project", known_args.project, "--region", known_args.region]

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    enable_fs = str2bool(known_args.enable_firestore)

    deadletter_path = (known_args.deadletter_path or "").strip()
    enable_gcs_dlq = bool(deadletter_path)
    deadletter_window_sec = max(int(known_args.deadletter_window_sec), 10)
    deadletter_trigger_sec = max(int(known_args.deadletter_trigger_sec), 5)

    # BigQuery schema for VALID events
    table_schema = (
        "sensor_id:STRING, "
        "temperature:FLOAT, "
        "humidity:FLOAT, "
        "timestamp:TIMESTAMP, "
        "source_type:STRING, "
        "processed_at:TIMESTAMP"
    )

    # BigQuery schema for DLQ rows
    dlq_schema = "raw:STRING, error:STRING, ingested_at:TIMESTAMP"

    with beam.Pipeline(options=pipeline_options) as p:
        outputs = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "ParseAndValidate"
            >> beam.ParDo(ParseAndValidate()).with_outputs(ParseAndValidate.DEADLETTER_TAG, main="main")
        )

        valid_msgs = outputs["main"]
        dlq_msgs = outputs[ParseAndValidate.DEADLETTER_TAG]

        # Valid -> BigQuery (FORCE STREAMING_INSERTS)
        _ = (
            valid_msgs
            | "WriteValidToBigQuery"
            >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

        # Optional Firestore
        if enable_fs:
            _ = (
                valid_msgs
                | "WriteStateToFirestore"
                >> beam.ParDo(WriteStateToFirestore(known_args.project_id, known_args.firestore_collection))
            )

        # DLQ -> BigQuery (FORCE STREAMING_INSERTS)
        _ = (
            dlq_msgs
            | "WriteDlqToBigQuery"
            >> beam.io.WriteToBigQuery(
                table=known_args.dlq_table,
                schema=dlq_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

        # DLQ -> GCS (OPTIONAL) using windowing + triggers (streaming-safe)
        #
        # Why:
        # - File-based sinks (WriteToText) use grouping/sharding internally.
        # - In streaming, global window + default trigger => Beam forbids GroupByKey.
        # Fix:
        # - Put DLQ into fixed windows and use a non-default trigger.
        #
        if enable_gcs_dlq:
            # Convert DLQ rows to newline-delimited JSON for files (jsonl)
            dead_jsonl = (
                dlq_msgs
                | "DlqToJsonl" >> beam.Map(lambda r: json.dumps(r, ensure_ascii=False))
                | "WindowDlqForGcs"
                >> beam.WindowInto(
                    FixedWindows(deadletter_window_sec),
                    trigger=Repeatedly(AfterProcessingTime(deadletter_trigger_sec)),
                    accumulation_mode=AccumulationMode.DISCARDING,
                    allowed_lateness=0,
                )
            )

            # Note:
            # - deadletter_path should be a *prefix*, not only a directory.
            #   Example: gs://bucket/dataflow/dlq/iot_readings_dlq
            # - Beam will create files like:
            #   <prefix>-00000-of-00005.jsonl (and multiple panes over time)
            _ = (
                dead_jsonl
                | "WriteDeadletterToGCS"
                >> beam.io.WriteToText(
                    file_path_prefix=deadletter_path,
                    file_name_suffix=".jsonl",
                    shard_name_template="-SSSSS-of-NNNNN",
                    num_shards=1,  # keep it simple; raise if DLQ volume grows
                )
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
