
import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, trim, window
from pyspark.sql.types import StructType, StructField, StringType

ROOT = Path(__file__).resolve().parent.parent

STATE_DB = ROOT / "streaming" / "hourly_noise_full.sqlite"
OUTPUT_CSV = ROOT / "output" / "hourly_noise_full.csv"
CHECKPOINT_PATH = ROOT / "streaming" / "checkpoints" / "hourly_noise_full"

OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)


# Create Spark session
spark = (
    SparkSession.builder
    .appName("NYC311-Streaming-Full")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Define the structure of our JSON complaints
schema = StructType([
    StructField("unique_key", StringType()),
    StructField("created_date", StringType()),
    StructField("incident_zip", StringType()),
    StructField("complaint_type", StringType())
])

# Read messages from Kafka

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "nyc311-replay-full")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "100000")
    .option("minPartitions", "8")
    .load()
)


# Convert Kafka bytes into a JSON string
json_stream = kafka_stream.selectExpr(
    "CAST(value AS STRING) AS complaint_json"
)

# Parse JSON into separate columns
complaints = (
    json_stream
    .withColumn(
        "complaint",
        from_json(col("complaint_json"), schema)
    )
    .select("complaint.*")
)

# Convert the date string to a timestamp
complaints = complaints.withColumn(
    "created_at",
    to_timestamp(
        col("created_date"),
        "yyyy-MM-dd'T'HH:mm:ss"
    )
)

# Only keep noise complaints

noise_complaints = complaints.filter(
    col("complaint_type").startswith("Noise")
    & col("created_at").isNotNull()
    & col("incident_zip").isNotNull()
    & (trim(col("incident_zip")) != "")
)

hourly_noise = (
    noise_complaints
    .withWatermark("created_at", "2 hours")
    .groupBy(
        window("created_at", "1 hour"),
        "incident_zip"
    )
    .count()
)




# Save the current hourly counts as CSV

def save_hourly_updates(batch_df, batch_id):
    conn = sqlite3.connect(STATE_DB)

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hourly_counts (
                window_start_epoch INTEGER NOT NULL,
                window_end_epoch INTEGER NOT NULL,
                incident_zip TEXT NOT NULL,
                noise_count INTEGER NOT NULL,
                PRIMARY KEY (window_start_epoch, incident_zip)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS completed_batches (
                batch_id INTEGER PRIMARY KEY
            )
        """)

        already_saved = conn.execute(
            "SELECT 1 FROM completed_batches WHERE batch_id = ?",
            (batch_id,)
        ).fetchone()

        if already_saved:
            for _ in batch_df.toLocalIterator():
                pass
            print(f"Batch {batch_id} already saved")
            return

        updates = batch_df.select(
            col("window.start").cast("long").alias("start"),
            col("window.end").cast("long").alias("end"),
            col("incident_zip"),
            col("count")
        )

        conn.executemany("""
            INSERT INTO hourly_counts
                (window_start_epoch, window_end_epoch,
                 incident_zip, noise_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(window_start_epoch, incident_zip)
            DO UPDATE SET
                window_end_epoch = excluded.window_end_epoch,
                noise_count = excluded.noise_count
        """, (
            (row.start, row.end, row.incident_zip, row["count"])
            for row in updates.toLocalIterator()
        ))

        conn.execute(
            "INSERT INTO completed_batches (batch_id) VALUES (?)",
            (batch_id,)
        )
        conn.commit()

        print(f"Saved streaming batch {batch_id}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


query = (
    hourly_noise.writeStream
    .outputMode("update")
    .foreachBatch(save_hourly_updates)
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
spark.stop()

# Export final results to a single CSV
with sqlite3.connect(STATE_DB) as conn:
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow([
            "window_start",
            "window_end",
            "incident_zip",
            "noise_count"
        ])

        cursor = conn.execute("""
            SELECT window_start_epoch, window_end_epoch,
                   incident_zip, noise_count
            FROM hourly_counts
            ORDER BY window_start_epoch, incident_zip
        """)

        for start, end, zip_code, count in cursor:
            writer.writerow([
                datetime.fromtimestamp(start, timezone.utc).isoformat(),
                datetime.fromtimestamp(end, timezone.utc).isoformat(),
                zip_code,
                count
            ])

print(f"Final CSV saved to: {OUTPUT_CSV}")




