import argparse
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

from google.cloud import firestore


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None

    if isinstance(value, str):
        v = value.strip()
        if "T" not in v and " " in v:
            v = v.replace(" ", "T") + "Z"
        if v.endswith("Z") or "+" in v:
            return v
        return v + "Z"

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    return None


class ParseAndValidate(beam.DoFn):
    DEADLETTER_TAG = "deadletter"

    def process(self, element: bytes):
        try:
            payload = json.loads(element.decode("utf-8"))

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

            payload.setdefault("source_type", "streaming")

            ts = normalize_timestamp(payload.get("timestamp"))
            payload["timestamp"] = ts if ts else utc_now_iso()
            payload["processed_at"] = utc_now_iso()

            yield payload

        except Exception as e:
            yield pvalue.TaggedOutput(self.DEADLETTER_TAG, (element, str(e)))


class WriteStateToFirestore(beam.DoFn):
    def __init__(self, project_id: str, collection: str):
        self.project_id = project_id
        self.collection = collection
        self.db = None

    def setup(self):
        self.db = firestore.Client(project=self.project_id)

    def process(self, element: Dict[str, Any]):
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


def run(argv=None):
    parser = argparse.ArgumentParser()

    # Script args
    parser.add_argument("--input_topic", required=True, help="projects/<project>/topics/<topic>")
    parser.add_argument("--output_table", required=True, help="project:dataset.table")
    parser.add_argument("--project_id", required=True, help="Project ID (usado no Firestore client)")

    # Dataflow-required args
    parser.add_argument("--project", required=True, help="GCP project para o Dataflow")
    parser.add_argument("--region", required=True, help="Região do Dataflow")

    # Firestore controls
    parser.add_argument("--enable_firestore", action="store_true", help="Habilita escrita no Firestore")
    parser.add_argument("--firestore_collection", default="sensors", help="Coleção Firestore para estado do sensor")

    # Deadletter controls (optional)
    parser.add_argument("--deadletter_path", default="", help="gs://bucket/prefix (sem arquivo)")
    parser.add_argument(
        "--deadletter_window_sec",
        type=int,
        default=60,
        help="Tamanho da janela (segundos) para flush do deadletter em GCS (default=60)",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Ensure project & region are present in PipelineOptions
    pipeline_args = list(pipeline_args) + ["--project", known_args.project, "--region", known_args.region]

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    table_schema = (
        "sensor_id:STRING, "
        "temperature:FLOAT, "
        "timestamp:TIMESTAMP, "
        "source_type:STRING, "
        "processed_at:TIMESTAMP"
    )

    with beam.Pipeline(options=pipeline_options) as p:
        outputs = (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidate()).with_outputs(
                ParseAndValidate.DEADLETTER_TAG, main="main"
            )
        )

        valid_msgs = outputs["main"]
        dead_msgs: beam.PCollection[Tuple[bytes, str]] = outputs[ParseAndValidate.DEADLETTER_TAG]

        # BigQuery
        _ = (
            valid_msgs
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Firestore
        if known_args.enable_firestore:
            _ = valid_msgs | "WriteStateToFirestore" >> beam.ParDo(
                WriteStateToFirestore(known_args.project_id, known_args.firestore_collection)
            )

        # Deadletter -> GCS (windowed)
        if known_args.deadletter_path:
            window_sec = max(1, int(known_args.deadletter_window_sec))

            dead_json = (
                dead_msgs
                | "DeadletterToJson" >> beam.Map(
                    lambda x: json.dumps(
                        {
                            "raw": x[0].decode("utf-8", errors="replace"),
                            "error": x[1],
                            "ingested_at": utc_now_iso(),
                        }
                    )
                )
                # ✅ janelar para permitir escrita contínua
                | "WindowDeadletter" >> beam.WindowInto(FixedWindows(window_sec))
            )

            _ = dead_json | "WriteDeadletterToGCS" >> beam.io.WriteToText(
                file_path_prefix=known_args.deadletter_path.rstrip("/") + "/deadletter",
                file_name_suffix=".jsonl",
                shard_name_template="-SSSSS-of-NNNNN",
                # ✅ necessário para unbounded + GlobalWindow/triggering
                triggering_frequency=window_sec,
                # ✅ evita muitos shards em lab
                num_shards=1,
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
