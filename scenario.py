import os
import sys
import shutil
import subprocess
import importlib.util
import time

def _ensure(pkg, import_name=None):
    if importlib.util.find_spec(import_name or pkg) is None:
        print(f"Installing {pkg} …")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

_ensure("pyspark")
_ensure("pandas")
_ensure("pyarrow")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# ── Progress Listener ─────────────────────────────────────────────────────────

class ConsoleProgressListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"[{event.timestamp}] Query started: {event.name} (ID: {event.id})")

    def onQueryProgress(self, event):
        p = event.progress
        print(
            f"> [{p.name}] batch={p.batchId} "
            f"inputRows={p.numInputRows} "
            f"processedRowsPerSecond={p.processedRowsPerSecond:.1f}"
        )

    def onQueryTerminated(self, event):
        print(f"Query terminated: {event.id}")
        if event.exception:
            print(f"Error: {event.exception}")

# ── Spark Session ─────────────────────────────────────────────────────────────

def get_spark_session():
    MINIO_USER = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    MINIO_PASS = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")

    spark = (
        SparkSession.builder
        .appName("project2_pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", MINIO_USER)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", MINIO_PASS)
        .config("spark.sql.catalog.lakehouse.s3.region", "us-east-1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ConsoleProgressListener())
    return spark

# ── Table DDL ─────────────────────────────────────────────────────────────────

def create_tables(spark, db):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_earliest (
            vendor_id_key   STRING,
            json_payload    STRING,
            topic           STRING,
            partition       INT,
            offset          LONG,
            kafka_timestamp TIMESTAMP
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_latest (
            vendor_id_key   STRING,
            json_payload    STRING,
            topic           STRING,
            partition       INT,
            offset          LONG,
            kafka_timestamp TIMESTAMP
        ) USING iceberg
    """)

# Bronze
def implement_bronze(spark, bootstrap, topic, db, checkpoint_path, table_name, starting_offsets):
    print(f"Launching Bronze Layer (with {starting_offsets} offset)")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_df = raw_stream.selectExpr(
        "CAST(key AS STRING) AS vendor_id_key",
        "CAST(value AS STRING) AS json_payload",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    )

    return (
        bronze_df.writeStream
        .queryName("bronze_ingestion")
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", os.path.join(checkpoint_path, table_name))
        .toTable(f"{db}.{table_name}")
    )

# helper
def wait_for_rows(spark, table_name, target_count, query, poll_interval=2):
    while True:
        count = spark.table(table_name).count()
        if count >= target_count:
            query.stop()
            print(f"{table_name} reached {target_count} rows, stopped.")
            break
        time.sleep(poll_interval)

# ── Main ──────────────────────────────────────────────────────────────────────

def reset_pipeline(spark, db_name, checkpoint_base):
    """Drop all tables and clear checkpoints for a clean start."""
    print("Resetting pipeline: dropping tables and clearing checkpoints...")
    for table in ["bronze_earliest", "bronze_latest"]:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table}")
            print(f"Dropped {db_name}.{table}")
        except Exception as e:
            print(f"Warning: {e}")
    for suffix in ["_earliest", "_latest"]:
        path = os.path.join(checkpoint_base + suffix)
        print(f"About to delete checkpoint at: {path}, exists={os.path.exists(path)}")
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Cleared checkpoint: {path}")
    print("Reset complete.\n")

def main():
    bootstrap = "kafka:9092"
    topic = "taxi-trips"
    db_name = "lakehouse.taxi"

    do_reset = "--reset" in sys.argv

    script_dir = os.path.dirname(os.path.abspath(__file__))
    checkpoint_base = os.path.join(script_dir, "checkpoints")
    checkpoint_path = f"file:{checkpoint_base}"

    spark = get_spark_session()

    if do_reset:
        reset_pipeline(spark, db_name, checkpoint_base)

    create_tables(spark, db_name)

    query_earliest = implement_bronze(spark, bootstrap, topic, db_name, checkpoint_path + "_earliest","bronze_earliest", "earliest")
    wait_for_rows(spark, f"{db_name}.bronze_earliest", 500, query_earliest)

    query_latest = implement_bronze(spark, bootstrap, topic, db_name, checkpoint_path + "_latest","bronze_latest", "latest")
    wait_for_rows(spark, f"{db_name}.bronze_latest", 100, query_latest)

    # Print final counts
    earliest_count = spark.table(f"{db_name}.bronze_earliest").count()
    latest_count = spark.table(f"{db_name}.bronze_latest").count()
    print(f"Rows in bronze_earliest: {earliest_count}")
    print(f"Rows in bronze_latest: {latest_count}")

if __name__ == "__main__":
    main()