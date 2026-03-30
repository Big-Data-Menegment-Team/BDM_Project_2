# Project 2: Streaming Lakehouse Pipeline

## 1. Medallion layer schemas

### Bronze

```sql
CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze (
  vendor_id_key   STRING,
  json_payload    STRING,
  topic           STRING,
  partition       INT,
  offset          LONG,
  kafka_timestamp TIMESTAMP
) USING iceberg
```

The Bronze layer stores every Kafka message. Every message from Kafka lands here exactly as-is. We store the full JSON body in `json_payload` without parsing it. This way, if we ever mess up the parsing logic later, we can always go back to the raw data and reprocess it. The `partition` and `offset` columns come from Kafka and let us track exactly which messages we have already consumed.

### Silver

```sql
CREATE TABLE IF NOT EXISTS lakehouse.taxi.silver (
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
) USING iceberg
PARTITIONED BY (pickup_date)
```

Silver takes the raw JSON from Bronze and turns it into a proper structured table. Compared to Bronze:

- **Parses** the JSON string into 20 typed columns (integers, doubles, strings).
- **Adds derived columns** we computed ourselves: `pickup_datetime` and `dropoff_datetime` as proper timestamps, `pickup_date` for partitioning, and `trip_duration_min` (dropoff minus pickup in minutes).
- **Enriches** each row with human-readable zone and borough names by joining the zone lookup table on `PULocationID` and `DOLocationID`.
- **Filters out** bad rows (nulls, negative fares, zero-distance trips, etc.) and **removes duplicates**.
- **Partitioned by** `pickup_date` so that queries filtering by date only scan the relevant files.

### Gold

```sql
CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold (
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
) USING iceberg
PARTITIONED BY (pickup_zone)
```

Gold collapses individual trips into hourly summaries per pickup-dropoff zone pair. Instead of millions of individual trip rows, you get answers like "Between 8-9 AM, 47 trips started in the East Village with average fare $12.50". This is the layer dashboards and analysts would query directly.

---

## 2. Cleaning rules and enrichment

We use the same cleaning rules as in Project 1 to keep things consistent.

| # | Rule | What we check | Why |
|---|------|---------------|-----|
| 1 | Non-null timestamps | `pickup_datetime` and `dropoff_datetime` must exist | Can't analyze a trip if we don't know when it happened. |
| 2 | Chronological trip | Dropoff must be after pickup | A trip that ends before it starts is clearly bad data. |
| 3 | Positive distance | `trip_distance > 0` | A taxi trip with zero distance didn't actually go anywhere. |
| 4 | Valid passenger count | `passenger_count > 0` (nulls are allowed through) | A trip with 0 passengers doesn't make sense. Some rows have null passenger count, which we allow since the trip itself might still be valid. |
| 5 | Positive fare | `fare_amount > 0` and `total_amount > 0` | Free or negative-fare trips are data errors. |
| 6 | Valid location IDs | `PULocationID` and `DOLocationID` must exist | We need these to join the zone lookup table and know where the trip happened. |
| 7 | Deduplication | See explanation below | The same trip can arrive in Kafka more than once (e.g., producer retry). We don't want to count it twice. |

### How deduplication works

We define a "trip fingerprint" as: `(pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, trip_distance, VendorID)`. If two rows share all six of these values, they're the same trip.

We use Spark's `dropDuplicatesWithinWatermark` with a 10-minute watermark on `kafka_timestamp`. Here's the simple version of what that means:

- Spark keeps a memory of trip fingerprints it has seen recently.
- When a new row arrives, Spark checks: "have I seen this exact fingerprint in the last 10 minutes?" If yes, it's a duplicate and gets dropped. If no, it passes through.
- After 10 minutes, Spark forgets old fingerprints to avoid running out of memory.

The 10-minute window is generous enough to catch duplicates from producer retries, but short enough to keep memory usage reasonable.

### A note on type parsing

We ran into an issue where `passenger_count` and `RatecodeID` were always NULL in Silver. It turned out that pandas (which the producer uses) converts nullable integer columns to floats when serializing to JSON. So the Kafka message has `"passenger_count": 1.0` instead of `"passenger_count": 1`. Spark's `from_json` is strict: if you tell it to expect an integer but the JSON has `1.0`, parsing fails and the field becomes NULL. The fix was to declare these fields as `DoubleType` in the parsing schema and then cast to `INT` afterwards.

Also, `from_json` matches JSON keys case-sensitively. The field names in the schema must exactly match what the producer sends (e.g., `Airport_fee` with a capital A, not `airport_fee`).

### Enrichment

The zone lookup file (`taxi_zone_lookup.parquet`, 265 rows) maps location IDs to human-readable zone and borough names. We broadcast-join it twice: once for pickup location, once for dropoff. We use left joins so that trips with unknown location IDs don't get dropped - they just get NULL zone names.

---

## 3. Streaming configuration

| Setting | Value | Why we chose this |
|---------|-------|-------------------|
| **Checkpoint path** | `/checkpoints/{bronze,silver,gold}` | Each layer gets its own checkpoint directory. This is what makes restarts safe: Spark writes down "I've processed up to offset X" so it doesn't re-read old data after a restart. |
| **Trigger (Bronze & Silver)** | `processingTime="10 seconds"` | A micro-batch every 10 seconds. The producer sends ~5 events/second, so this gives us a good balance between latency (not waiting too long) and efficiency (not processing one row at a time). |
| **Trigger (Gold)** | `processingTime="30 seconds"` | Gold is heavier because it rewrites the entire aggregation table each time. Running it less often reduces how much work we're doing. |
| **Output mode (Bronze & Silver)** | `append` | Each row is independent - we just keep adding new rows. |
| **Output mode (Gold)** | `complete` | Gold uses `groupBy` to compute aggregations. Since the aggregation results change as new data arrives (e.g., the trip count for a zone increases), we need to rewrite the entire result table each time. That's what `complete` mode does. |
| **Watermark (Silver)** | `kafka_timestamp`, 10 minutes | Tells Spark how long to remember trip fingerprints for deduplication. After 10 minutes, it's safe to forget them. |
| **Watermark (Gold)** | `pickup_datetime`, 1 hour | Tells Spark when it's safe to "close" an hourly window. We give 1 hour of slack in case events arrive out of order. |

### Why `stream-from-timestamp=0`?

Silver reads from the Bronze Iceberg table, and Gold reads from Silver. By default, Iceberg streaming only picks up snapshots (new data) that are committed **after** the stream starts. But in our pipeline, all three layers start at the same time. So when Silver starts, Bronze might already have data from a few seconds ago that Silver would miss.

Setting `stream-from-timestamp=0` tells Iceberg: "start reading from the very beginning, don't skip anything." On the first run, this means Silver reads all of Bronze's data. On restarts, the checkpoint remembers what was already processed, so nothing gets re-read.

---

## 4. Gold table partitioning strategy

**Partition column:** `pickup_zone`

The most common way to query the Gold table is by zone: "show me trips starting from the East Village" or "what's the revenue for JFK Airport?". By partitioning on `pickup_zone`, Iceberg can skip all files for irrelevant zones when you filter, which makes these queries much faster.

We chose `pickup_zone` over a time-based partition because:
1. Gold is written in `complete` mode, which rewrites the entire table each trigger. Time-based partitioning wouldn't help with writes since everything gets rewritten anyway.
2. There are 265 taxi zones, which is a reasonable number of partitions (not too many, not too few).
3. Zone-based filtering is the main query pattern for this kind of aggregation table.

### Iceberg snapshot history

Each Gold snapshot represents one full rewrite. You can see the history with:

```sql
SELECT snapshot_id, committed_at, operation,
       summary['added-records']  AS added_records,
       summary['total-records']  AS total_records
FROM lakehouse.taxi.gold.snapshots
ORDER BY committed_at DESC LIMIT 10;
```

Example output:

| snapshot_id         | committed_at            | operation | added_records | total_records |
|---------------------|-------------------------|-----------|---------------|---------------|
| 2102944575022366262 | 2026-03-30 17:56:31.04  | overwrite | 167           | 167           |
| 3143716333390821295 | 2026-03-30 17:56:00.938 | overwrite | 167           | 167           |
| 1448028090303523389 | 2026-03-30 17:55:31.695 | overwrite | 98            | 98            |
| 5327270769243424461 | 2026-03-30 17:53:40.733 | delete    | NULL          | 0             |

The first entry is a `delete` (empty table created). Then the first overwrite has only 98 rows (Silver hadn't finished processing yet), but subsequent ones stabilize at 167 as all Silver data became available. The `overwrite` operation confirms that complete mode replaces the entire table each trigger.

---

## 5. Restart proof

Here's what we did to verify that stopping and restarting the pipeline doesn't create duplicate rows:

1. Produced 200 messages into Kafka (`produce.py --limit 200`) and ran the pipeline until everything was processed.
2. Stopped the pipeline with Ctrl+C.
3. Checked the row counts:
   - **Bronze: 200** (every Kafka message stored)
   - **Silver: 185** (15 rows filtered out by cleaning rules)
   - **Gold: 167** (hourly aggregations)
   - Duplicate checks: **NONE** on both layers
4. Restarted the pipeline (`python pipeline.py`) **without** producing any new data.
5. Waited ~30 seconds, stopped again, and checked:
   - **Bronze: 200** (same, no new rows)
   - **Silver: 185** (same)
   - **Gold: 167** (same)
   - No new snapshots were created.

This works because the checkpoint stores the last Kafka offset that was successfully processed. When the pipeline restarts, Spark reads "I already consumed up to offset 200" from the checkpoint and picks up from there. Since we didn't produce any new messages, there's nothing new to read.

The `--reset` flag can be used to wipe everything (drop tables + delete checkpoints) for a clean start.

---

## 6. Custom scenario

_Explain and/or show how you solved the custom scenario from the GitHub issue._

## 7. How to run

```bash
# Step 1: Start infrastructure
docker compose up -d

# Step 2: Create the Kafka topic (run once)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic taxi-trips --partitions 3 --replication-factor 1 --if-not-exists

# Step 3: Start the producer
docker exec project2_jupyter python /home/jovyan/project/produce.py --loop
# Or with a limit for testing:
# docker exec project2_jupyter python /home/jovyan/project/produce.py --limit 200

# Step 4: Run the streaming pipeline
docker exec project2_jupyter python /home/jovyan/project/pipeline.py
# Use --reset for a clean start (drops tables + clears checkpoints):
# docker exec project2_jupyter python /home/jovyan/project/pipeline.py --reset

# Step 5: Verify data and check for duplicates
docker exec project2_jupyter python /home/jovyan/project/verify_data.py
```