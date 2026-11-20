"""Save the answers to Q1 and Q2 as CSV files for the serving database.

benchmark_layouts.py (Part 1) times Q1 and Q2 but only prints the answers.
This script runs the same two queries once on the Parquet data and writes
the results to output/, where src/serve/load_results.py (Part 3) loads them:

  output/q1_complaints_by_borough.csv   every (borough, complaint type) count
  output/q2_agency_close_time.csv       average closing time per agency

Q1 keeps every complaint type, not only the top five, so the database can
answer "top N in my borough" for any N.

Run from the project root:  python src/batch/export_batch_results.py
"""

import csv
import sys
from pathlib import Path

# Make src/ importable when this file is run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.spark_config import DATA_PROCESSING, OUTPUT_DIR, configure_hadoop

PARQUET_PATH = DATA_PROCESSING / "311_Service_Requests_For_2026.parquet"
Q1_CSV = OUTPUT_DIR / "q1_complaints_by_borough.csv"
Q2_CSV = OUTPUT_DIR / "q2_agency_close_time.csv"

# NYC 311 dates look like 01/31/2026 11:05:00 PM.
TIMESTAMP_FORMAT = "MM/dd/yyyy hh:mm:ss a"


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    if not PARQUET_PATH.is_file():
        # Not fatal: load_results.py still loads any result files already in
        # output/, so the stream results can be served without the batch data.
        print(f"SKIPPED batch export: {PARQUET_PATH} not found.\n"
              "Run clean_requests.py and convert_to_parquet.py first, or "
              "copy the Parquet file into data/processing/.")
        return

    from pyspark.sql import SparkSession, functions as F

    configure_hadoop()
    spark = (
        SparkSession.builder
        .appName("NYC311-Export-Batch-Results")
        # Created and Closed Dates are New York local times. Reading them in
        # New York time makes durations across daylight saving changes correct.
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def timestamp(column):
        # Same parsing as benchmark_layouts.py.
        return F.coalesce(
            F.to_timestamp(F.col(column), TIMESTAMP_FORMAT),
            F.to_timestamp(F.col(column)),
        )

    try:
        df = spark.read.parquet(str(PARQUET_PATH)).select(
            F.trim(F.col("Borough")).alias("borough"),
            F.trim(F.col("Problem (formerly Complaint Type)"))
             .alias("complaint_type"),
            F.trim(F.col("Agency")).alias("agency"),
            timestamp("Created Date").alias("created_ts"),
            timestamp("Closed Date").alias("closed_ts"),
        ).cache()
        rows_read = df.count()

        # Q1: requests per (borough, complaint type).
        q1_input = df.filter(
            F.col("borough").isNotNull() & (F.col("borough") != "")
            & F.col("complaint_type").isNotNull()
            & (F.col("complaint_type") != "")
        )
        q1 = (
            q1_input.groupBy("borough", "complaint_type").count()
            .orderBy("borough", F.desc("count"), "complaint_type")
            .collect()
        )
        write_csv(Q1_CSV, ["borough", "complaint_type", "request_count"],
                  [(r["borough"], r["complaint_type"], r["count"]) for r in q1])

        # Q2: average hours from Created Date to Closed Date, per agency.
        # Only closed requests with a closing time after the creation time.
        q2_input = df.filter(
            F.col("agency").isNotNull() & (F.col("agency") != "")
            & F.col("created_ts").isNotNull()
            & F.col("closed_ts").isNotNull()
            & (F.col("closed_ts") >= F.col("created_ts"))
        )
        q2 = (
            q2_input.withColumn(
                "close_hours",
                (F.col("closed_ts").cast("long")
                 - F.col("created_ts").cast("long")) / 3600.0,
            )
            .groupBy("agency")
            .agg(F.round(F.avg("close_hours"), 2).alias("avg_close_hours"),
                 F.count("*").alias("closed_requests"))
            .orderBy("agency")
            .collect()
        )
        write_csv(Q2_CSV, ["agency", "avg_close_hours", "closed_requests"],
                  [(r["agency"], r["avg_close_hours"], r["closed_requests"])
                   for r in q2])

        # Part 4: rows in and rows used at this stage.
        q1_used = q1_input.count()
        q2_used = q2_input.count()
        print("--- Batch export report ---")
        print(f"Rows read from Parquet:  {rows_read:>10,}")
        print(f"Q1 rows used:            {q1_used:>10,}  "
              f"(skipped {rows_read - q1_used:,} with no borough or type)")
        print(f"Q2 rows used:            {q2_used:>10,}  "
              f"(skipped {rows_read - q2_used:,} open, undated or closed "
              "before created)")
        print(f"Wrote {len(q1):,} rows to {Q1_CSV.name}")
        print(f"Wrote {len(q2):,} rows to {Q2_CSV.name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
