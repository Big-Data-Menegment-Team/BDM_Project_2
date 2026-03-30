import os
import sys
import shutil
import subprocess
import importlib.util

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
        CREATE TABLE IF NOT EXISTS {db}.bronze (
            vendor_id_key   STRING,
            json_payload    STRING,
            topic           STRING,
            partition       INT,
            offset          LONG,
            kafka_timestamp TIMESTAMP
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.silver (
            VendorID              INT,
            tpep_pickup_datetime  STRING,
            tpep_dropoff_datetime STRING,
            passenger_count       INT,
            trip_distance         DOUBLE,
            RatecodeID            INT,
            store_and_fwd_flag    STRING,
            PULocationID          INT,
            DOLocationID          INT,
            payment_type          INT,
            fare_amount           DOUBLE,
            extra                 DOUBLE,
            mta_tax               DOUBLE,
            tip_amount            DOUBLE,
            tolls_amount          DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount          DOUBLE,
            congestion_surcharge  DOUBLE,
            Airport_fee           DOUBLE,
            cbd_congestion_fee    DOUBLE,
            kafka_timestamp       TIMESTAMP,
            pickup_datetime       TIMESTAMP,
            dropoff_datetime      TIMESTAMP,
            pickup_date           DATE,
            trip_duration_min     DOUBLE,
            pickup_borough        STRING,
            pickup_zone           STRING,
            dropoff_borough       STRING,
            dropoff_zone          STRING
        )
        USING iceberg
        PARTITIONED BY (pickup_date)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db}.gold (
            hour               TIMESTAMP,
            hour_end           TIMESTAMP,
            pickup_zone        STRING,
            pickup_borough     STRING,
            dropoff_zone       STRING,
            dropoff_borough    STRING,
            trip_count         BIGINT,
            total_revenue      DOUBLE,
            avg_fare           DOUBLE,
            avg_distance_miles DOUBLE,
            avg_duration_min   DOUBLE,
            total_passengers   BIGINT,
            revenue_per_trip   DOUBLE
        )
        USING iceberg
        PARTITIONED BY (pickup_zone)
    """)

# Bronze
def implement_bronze(spark, bootstrap, topic, db, checkpoint_path):
    print("Launching Bronze Layer (Kafka -> Raw Iceberg)...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
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
        .option("checkpointLocation", os.path.join(checkpoint_path, "bronze"))
        .toTable(f"{db}.bronze")
    )

# Silver

def implement_silver(spark, db, checkpoint_path, zones_path):
    print("Launching Silver Layer (Parsed & Enriched)...")

    schema = StructType([
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", StringType()),
        StructField("tpep_dropoff_datetime", StringType()),
        StructField("passenger_count", DoubleType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", DoubleType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra",DoubleType()),
        StructField("mta_tax",DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
    ])

    # Important: explicit Iceberg streaming source + stream-from-timestamp=0
    # so Silver can consume existing Bronze snapshots after startup.
    bronze_stream = (
        spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "0")
        .load(f"{db}.bronze")
    )

    parsed_df = (
        bronze_stream
        .withColumn("data", F.from_json(F.col("json_payload"), schema))
        .select("data.*", "kafka_timestamp")
    )

    # 1. Parsing and Casting
    df_casted = (
        parsed_df
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
        .withColumn("pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime").cast("timestamp")))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp")) - 
             F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))) / 60.0
        )
    )

    # 2. Cleaning rules, used same logic as project1 cleaning task
    df_cleaned = df_casted.filter(
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        (F.col("trip_distance") > 0) &
        (F.coalesce(F.col("passenger_count"), F.lit(1)) > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("total_amount") > 0) &
        F.col("PULocationID").isNotNull() &
        F.col("DOLocationID").isNotNull()
    )

    # 3. Deduplication
    deduplication_key = [
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "VendorID",
    ]

    df_deduped = (
        df_cleaned
        .withWatermark("kafka_timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(deduplication_key)
    )

    # 4. Zone Enrichment
    zones_raw = spark.read.parquet(zones_path)
    zones = zones_raw.select(
        F.col("LocationID").cast("int"),
        F.col("Zone").alias("zone_name"),
        F.col("Borough").alias("borough")
    )

    pickup_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("zone_name").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough")
    )

    dropoff_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("zone_name").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough")
    )

    df_enriched = (
        df_deduped
        .join(F.broadcast(pickup_zones), on="PULocationID", how="left")
        .join(F.broadcast(dropoff_zones), on="DOLocationID", how="left")
    )

    final_df = df_enriched.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
        "cbd_congestion_fee",
        "kafka_timestamp",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "trip_duration_min",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone"
    )

    return (
        final_df.writeStream
        .queryName("silver_parsing")
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", os.path.join(checkpoint_path, "silver"))
        .toTable(f"{db}.silver")
    )

# ── Gold ──────────────────────────────────────────────────────────────────────

def implement_gold(spark, db, checkpoint_path):
    print("Launching Gold Layer (Hourly Aggregations)...")

    silver_stream = (
        spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "0")
        .load(f"{db}.silver")
    )

    gold_df = (
        silver_stream
        .withWatermark("pickup_datetime", "1 hour")
        .groupBy(
            F.window("pickup_datetime", "1 hour").alias("window"),
            "pickup_zone",
            "pickup_borough",
            "dropoff_zone",
            "dropoff_borough",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance_miles"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.sum("passenger_count").alias("total_passengers"),
        )
        .select(
            F.col("window.start").alias("hour"),
            F.col("window.end").alias("hour_end"),
            "pickup_zone",
            "pickup_borough",
            "dropoff_zone",
            "dropoff_borough",
            "trip_count",
            "total_revenue",
            "avg_fare",
            "avg_distance_miles",
            "avg_duration_min",
            "total_passengers",
            (F.col("total_revenue") / F.col("trip_count")).alias("revenue_per_trip"),
        )
    )

    return (
        gold_df.writeStream
        .queryName("gold_aggregations")
        .format("iceberg")
        .outputMode("complete")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", os.path.join(checkpoint_path, "gold"))
        .toTable(f"{db}.gold")
    )

# ── Main ──────────────────────────────────────────────────────────────────────

def reset_pipeline(spark, db_name, checkpoint_base):
    """Drop all tables and clear checkpoints for a clean start."""
    print("Resetting pipeline: dropping tables and clearing checkpoints...")
    for table in ["bronze", "silver", "gold"]:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table}")
            print(f"Dropped {db_name}.{table}")
        except Exception as e:
            print(f"Warning: {e}")
    if os.path.exists(checkpoint_base):
        shutil.rmtree(checkpoint_base)
        print(f"Cleared checkpoints: {checkpoint_base}")
    print("Reset complete.\n")

def main():
    bootstrap = "kafka:9092"
    topic = "taxi-trips"
    db_name = "lakehouse.taxi"

    do_reset = "--reset" in sys.argv

    script_dir = os.path.dirname(os.path.abspath(__file__))
    zones_path = os.path.join(script_dir, "data/taxi_zone_lookup.parquet")
    checkpoint_base = os.path.join(script_dir, "checkpoints")
    checkpoint_path = f"file:{checkpoint_base}"

    if not os.path.exists(zones_path):
        sys.exit(f"Zone lookup not found: {zones_path}\nPlace taxi_zone_lookup.parquet in data/")

    spark = get_spark_session()

    if do_reset:
        reset_pipeline(spark, db_name, checkpoint_base)

    os.makedirs(checkpoint_base, exist_ok=True)
    create_tables(spark, db_name)

    implement_bronze(spark, bootstrap, topic, db_name, checkpoint_path)
    implement_silver(spark, db_name, checkpoint_path, zones_path)
    implement_gold(spark, db_name, checkpoint_path)

    print("STREAMING PIPELINE ACTIVE - Bronze -> Silver -> Gold")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping all streaming queries...")
        for q in spark.streams.active:
            q.stop()
        print("Done.")

if __name__ == "__main__":
    main()