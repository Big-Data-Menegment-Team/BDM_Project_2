import os
import sys
import subprocess
import importlib.util

def _ensure(pkg, import_name=None):
    if importlib.util.find_spec(import_name or pkg) is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])

_ensure("pyspark")

from pyspark.sql import SparkSession

def get_spark():
    MINIO_USER = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    MINIO_PASS = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")

    spark = (
        SparkSession.builder
        .appName("verification")
        .master("local[2]")
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
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def check_table(spark, table):
    print(f"\n{'─'*55}")
    print(f"TABLE: {table}")
    print(f"{'─'*55}")

    # Row count
    try:
        count = spark.table(table).count()
        print(f"Row count : {count:,}")
    except Exception as e:
        print(f"Row count: Error - {e}")
        return

    # Sample rows
    try:
        print("Sample (3 rows):")
        spark.table(table).limit(3).show(truncate=80, vertical=False)
    except Exception as e:
        print(f"Sample: Error - {e}")

    # Snapshot history
    try:
        print("Snapshot history:")
        spark.sql(f"""
            SELECT snapshot_id,
                   committed_at,
                   operation,
                   summary['added-records'] AS added_records,
                   summary['total-records'] AS total_records
            FROM {table}.snapshots
            ORDER BY committed_at DESC
            LIMIT 10
        """).show(truncate=False)
    except Exception as e:
        print(f"Snapshots : Error - {e}")

def main():
    print("Starting verification Spark session...")
    spark = get_spark()
    print("Connected.\n")

    for table in [
        "lakehouse.taxi.bronze",
        "lakehouse.taxi.silver",
        "lakehouse.taxi.gold",
    ]:
        check_table(spark, table)

    # ── Duplicate checks ──
    print(f"\n{'='*55}")
    print("DUPLICATE CHECKS")
    print(f"{'='*55}")

    # Bronze: each Kafka (partition, offset) pair should appear exactly once
    try:
        bronze_dupes = spark.sql("""
            SELECT partition, offset, COUNT(*) AS cnt
            FROM lakehouse.taxi.bronze
            GROUP BY partition, offset
            HAVING COUNT(*) > 1
        """)
        dup_count = bronze_dupes.count()
        if dup_count == 0:
            print("Bronze duplicates (by partition+offset): NONE")
        else:
            print(f"Bronze duplicates (by partition+offset): {dup_count} FOUND!")
            bronze_dupes.show(10, truncate=False)
    except Exception as e:
        print(f"Bronze duplicate check: Error - {e}")

    # Silver: check the dedup key used in the pipeline
    try:
        silver_dupes = spark.sql("""
            SELECT pickup_datetime, dropoff_datetime, PULocationID,
                   DOLocationID, trip_distance, VendorID, COUNT(*) AS cnt
            FROM lakehouse.taxi.silver
            GROUP BY pickup_datetime, dropoff_datetime, PULocationID,
                     DOLocationID, trip_distance, VendorID
            HAVING COUNT(*) > 1
        """)
        dup_count = silver_dupes.count()
        if dup_count == 0:
            print("Silver duplicates (by dedup key): NONE")
        else:
            print(f"Silver duplicates (by dedup key): {dup_count} FOUND!")
            silver_dupes.show(10, truncate=False)
    except Exception as e:
        print(f"Silver duplicate check: Error - {e}")

    print(f"\n{'='*55}")
    print("SILVER: partition summary by pickup_date")
    print(f"{'='*55}")
    try:
        spark.sql("""
            SELECT pickup_date, COUNT(*) AS rows
            FROM lakehouse.taxi.silver
            GROUP BY pickup_date
            ORDER BY pickup_date
        """).show(50, truncate=False)
    except Exception as e:
        print(f"Error - {e}")

    print(f"\n{'='*55}")
    print("GOLD: top zones by trip count")
    print(f"{'='*55}")
    try:
        spark.sql("""
            SELECT pickup_zone, pickup_borough,
                   SUM(trip_count)    AS total_trips,
                   ROUND(SUM(total_revenue), 2) AS total_revenue
            FROM lakehouse.taxi.gold
            GROUP BY pickup_zone, pickup_borough
            ORDER BY total_trips DESC
            LIMIT 10
        """).show(truncate=False)
    except Exception as e:
        print(f"Error - {e}")

    spark.stop()
    print("\nDone.")

if __name__ == "__main__":
    main()