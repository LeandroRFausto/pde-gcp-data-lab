#!/usr/bin/env python3
"""
spark_batch_ingest.py

Dataproc (Spark) batch job:
- Input (Landing, GCS):
  gs://<landing_bucket>/<metadata_object>
  gs://<landing_bucket>/<history_object>

- Output (Raw, GCS Parquet):
  gs://<raw_bucket>/<raw_output_prefix>/dims/dim_sensors/
  gs://<raw_bucket>/<raw_output_prefix>/facts/iot_readings_batch/ (partitioned by event_date=YYYY-MM-DD)

Fact adds:
- ingestion_time (UTC)
- batch_id
- source = "batch"
"""

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--landing_bucket", required=True)
    p.add_argument("--raw_bucket", required=True)

    p.add_argument("--metadata_object", required=True)  # ex: batch_input/metadata/sensors_metadata.csv
    p.add_argument("--history_object", required=True)   # ex: batch_input/history/historical_logs.csv

    # ✅ novo (opcional)
    p.add_argument(
        "--raw_output_prefix",
        default="",
        help="Prefixo dentro do bucket RAW. Ex: 'readings/v1' (sem gs://). Default: ''",
    )

    p.add_argument("--batch_id", default=None)

    return p.parse_args()


def _normalize_prefix(prefix: str) -> str:
    """
    Normaliza prefixo para montar paths sem //.
    - "" -> ""
    - "readings/" -> "readings"
    - "/readings/v1/" -> "readings/v1"
    """
    if prefix is None:
        return ""
    p = prefix.strip().strip("/")
    return p


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("spark-batch-ingest-legacy-csv")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    batch_id = args.batch_id or datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    ingestion_time_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # -----------------------
    # Input paths (GCS)
    # -----------------------
    metadata_path = f"gs://{args.landing_bucket}/{args.metadata_object}"
    history_path = f"gs://{args.landing_bucket}/{args.history_object}"

    # -----------------------
    # Schemas
    # -----------------------
    schema_metadata = T.StructType([
        T.StructField("sensor_id", T.StringType(), True),
        T.StructField("location", T.StringType(), True),
        T.StructField("model", T.StringType(), True),
        T.StructField("install_date", T.StringType(), True),
    ])

    schema_history = T.StructType([
        T.StructField("sensor_id", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("temperature", T.StringType(), True),
    ])

    # -----------------------
    # Read CSVs
    # -----------------------
    df_meta_raw = (
        spark.read
        .option("header", "true")
        .schema(schema_metadata)
        .csv(metadata_path)
    )

    df_hist_raw = (
        spark.read
        .option("header", "true")
        .schema(schema_history)
        .csv(history_path)
    )

    # -----------------------
    # DIM: sensors (overwrite = "truncate")
    # -----------------------
    df_dim_sensors = (
        df_meta_raw
        .withColumn("sensor_id", F.trim(F.col("sensor_id")))
        .withColumn("location", F.trim(F.col("location")))
        .withColumn("model", F.trim(F.col("model")))
        .withColumn("install_date", F.to_date(F.col("install_date"), "yyyy-MM-dd"))
        .filter(F.col("sensor_id").isNotNull() & (F.col("sensor_id") != ""))
        .dropDuplicates(["sensor_id"])
    )

    # -----------------------
    # FACT: readings batch (append)
    # -----------------------
    ts_clean = F.regexp_replace(F.col("timestamp"), " ", "T")
    ts_clean = F.regexp_replace(ts_clean, "Z$", "")  # remove Z -> assumimos UTC
    ts_parsed = F.to_timestamp(ts_clean, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

    df_fact = (
        df_hist_raw
        .withColumn("sensor_id", F.trim(F.col("sensor_id")))
        .withColumn("timestamp", ts_parsed)
        .withColumn("temperature", F.col("temperature").cast("double"))
        .withColumn("ingestion_time", F.to_timestamp(F.lit(ingestion_time_str)))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source", F.lit("batch"))
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .filter(F.col("sensor_id").isNotNull() & (F.col("sensor_id") != ""))
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("temperature").isNotNull())
        .filter(F.col("event_date").isNotNull())
    )

    # -----------------------
    # Output paths (GCS raw)
    # -----------------------
    prefix = _normalize_prefix(args.raw_output_prefix)
    base = f"gs://{args.raw_bucket}"
    if prefix:
        base = f"{base}/{prefix}"

    dim_out = f"{base}/dims/dim_sensors"
    fact_out_base = f"{base}/facts/iot_readings_batch"

    # DIM = overwrite
    df_dim_sensors.write.mode("overwrite").parquet(dim_out)

    # FACT = append, partition by event_date -> hive style event_date=YYYY-MM-DD
    (
        df_fact
        .write
        .mode("append")
        .partitionBy("event_date")
        .parquet(fact_out_base)
    )

    spark.stop()


if __name__ == "__main__":
    main()
