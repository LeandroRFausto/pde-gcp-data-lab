#!/usr/bin/env python3
"""
batch_ingest.py

Batch ingestion (legacy CSV) -> BigQuery (Gold)

- Reads CSVs from Landing (GCS):
  gs://<input_bucket>/batch_input/metadata/*.csv
  gs://<input_bucket>/batch_input/history/*.csv

- Writes to BigQuery dataset (project:dataset):
  <output_dataset>.dim_sensors           (WRITE_TRUNCATE - full load)
  <output_dataset>.iot_readings_batch    (WRITE_APPEND   - incremental)

Adds ingestion metadata to the batch fact table:
- ingestion_time: TIMESTAMP (UTC)
- batch_id: STRING
- source: STRING ("batch")
"""

import argparse
import csv
import datetime as dt
import logging
from typing import Dict, Iterable, Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# -----------------------------
# BigQuery Schemas
# -----------------------------
SCHEMA_METADATA = "sensor_id:STRING, location:STRING, model:STRING, install_date:DATE"

SCHEMA_IOT_BATCH = (
    "sensor_id:STRING, "
    "timestamp:TIMESTAMP, "
    "temperature:FLOAT, "
    "ingestion_time:TIMESTAMP, "
    "batch_id:STRING, "
    "source:STRING"
)

# -----------------------------
# Helpers
# -----------------------------
def utc_now_rfc3339() -> str:
    # BigQuery accepts RFC3339 / ISO8601 timestamps, e.g. 2026-01-15T12:34:56.789Z
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_timestamp(value: str) -> str:
    """
    Accepts:
      - 2025-01-10T14:30:00
      - 2025-01-10T14:30:00Z
      - 2025-01-10 14:30:00
    Returns RFC3339 string. Assumes UTC if no timezone is present.
    """
    v = value.strip()
    v = v.replace(" ", "T")

    try:
        if v.endswith("Z"):
            # Already UTC
            return v
        # If timezone present like +00:00, keep it
        if "+" in v[10:] or "-" in v[10:]:
            # Might contain timezone offset after date part
            return dt.datetime.fromisoformat(v).astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
        # Naive -> assume UTC
        return dt.datetime.fromisoformat(v).replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        # Let caller decide to drop the row; return empty to signal invalid
        return ""


# -----------------------------
# DoFns
# -----------------------------
class ParseMetadata(beam.DoFn):
    def process(self, element: str) -> Iterable[Dict]:
        # Robust CSV parsing (handles quoted fields properly)
        try:
            row = next(csv.reader([element]))
            if not row or row[0].strip() == "sensor_id":
                return

            sensor_id = row[0].strip()
            location = row[1].strip() if len(row) > 1 else ""
            model = row[2].strip() if len(row) > 2 else ""
            install_date = row[3].strip() if len(row) > 3 else ""

            if not sensor_id:
                return

            yield {
                "sensor_id": sensor_id,
                "location": location,
                "model": model,
                "install_date": install_date,  # BigQuery DATE accepts 'YYYY-MM-DD'
            }
        except Exception as e:
            logging.exception("Erro ao parsear metadata. Linha=%r Erro=%s", element, e)
            return


class ParseHistory(beam.DoFn):
    def __init__(self, batch_id: str):
        self.batch_id = batch_id

    def process(self, element: str) -> Iterable[Dict]:
        try:
            row = next(csv.reader([element]))
            if not row or row[0].strip() == "sensor_id":
                return

            sensor_id = row[0].strip()
            ts_raw = row[1].strip() if len(row) > 1 else ""
            temp_raw = row[2].strip() if len(row) > 2 else ""

            if not sensor_id:
                return

            ts = normalize_timestamp(ts_raw)
            if not ts:
                return

            temperature = float(temp_raw)

            yield {
                "sensor_id": sensor_id,
                "timestamp": ts,
                "temperature": temperature,
                "ingestion_time": utc_now_rfc3339(),
                "batch_id": self.batch_id,
                "source": "batch",
            }
        except Exception as e:
            logging.exception("Erro ao parsear history. Linha=%r Erro=%s", element, e)
            return


# -----------------------------
# Main runner
# -----------------------------
def build_args(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_bucket",
        required=True,
        help="Bucket de entrada (Landing Zone), ex: quick-cache-484111-j4-landing-zone",
    )
    parser.add_argument(
        "--output_dataset",
        required=True,
        help="Dataset do BigQuery no formato project:dataset, ex: quick-cache-484111-j4:gold_layer",
    )
    parser.add_argument(
        "--batch_id",
        required=False,
        default=None,
        help="Identificador da carga (default: UTC timestamp). Ex: batch-20260115-120000",
    )

    # Deixa o Beam/Dataflow consumir os args restantes (runner, project, region, temp_location, etc.)
    return parser.parse_known_args(argv)


def run(argv=None):
    known_args, pipeline_args = build_args(argv)

    batch_id = known_args.batch_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Input paths on GCS
    metadata_path = f"gs://{known_args.input_bucket}/batch_input/metadata/*.csv"
    history_path = f"gs://{known_args.input_bucket}/batch_input/history/*.csv"

    # Output tables on BigQuery
    # output_dataset must be "project:dataset"
    table_metadata = f"{known_args.output_dataset}.dim_sensors"
    table_iot_batch = f"{known_args.output_dataset}.iot_readings_batch"

    # Better write method on Dataflow (works well for batch and avoids some edge cases)
    bq_write_method = beam.io.WriteToBigQuery.Method.FILE_LOADS

    with beam.Pipeline(options=pipeline_options) as p:
        # --- FLOW 1: DIMENSION (full load) ---
        (
            p
            | "Read Metadata CSV" >> beam.io.ReadFromText(metadata_path, skip_header_lines=1)
            | "Parse Metadata" >> beam.ParDo(ParseMetadata())
            | "Write dim_sensors (TRUNCATE)" >> beam.io.WriteToBigQuery(
                table=table_metadata,
                schema=SCHEMA_METADATA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=bq_write_method,
            )
        )

        # --- FLOW 2: FACT (incremental append) ---
        (
            p
            | "Read History CSV" >> beam.io.ReadFromText(history_path, skip_header_lines=1)
            | "Parse History" >> beam.ParDo(ParseHistory(batch_id))
            | "Write iot_readings_batch (APPEND)" >> beam.io.WriteToBigQuery(
                table=table_iot_batch,
                schema=SCHEMA_IOT_BATCH,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=bq_write_method,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
