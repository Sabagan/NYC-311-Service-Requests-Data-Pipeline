
from pathlib import Path
from datetime import datetime
import sqlite3
import sys
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.spark_config import DATA_PROCESSING, STREAM_DIR

PARQUET_PATH = DATA_PROCESSING / "311_Service_Requests_For_2026.parquet"
DB_PATH = STREAM_DIR / "events_sorted.sqlite"

DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"

columns = [
    "Unique Key",
    "Created Date",
    "Incident Zip",
    "Problem (formerly Complaint Type)"
]

# Rebuild our temporary sorting database
if not PARQUET_PATH.is_file():
    raise FileNotFoundError(f"Replay input not found: {PARQUET_PATH}")

DB_PATH.unlink(missing_ok=True)

conn = sqlite3.connect(DB_PATH)

try:
    conn.execute("""
        CREATE TABLE complaints (
            created_date TEXT,
            unique_key TEXT,
            incident_zip TEXT,
            complaint_type TEXT
        )
    """)

    parquet_file = pq.ParquetFile(PARQUET_PATH)

    total = 0
    invalid_dates = 0

    # Read the Parquet file in batches
    for batch in parquet_file.iter_batches(
        batch_size=10000,
        columns=columns
    ):
        records = []

        for row in batch.to_pylist():
            try:
                created_date = datetime.strptime(
                    row["Created Date"], DATE_FORMAT
                ).isoformat()
            except (ValueError, TypeError):
                invalid_dates += 1
                continue

            records.append((
                created_date,
                row["Unique Key"],
                row["Incident Zip"],
                row["Problem (formerly Complaint Type)"]
            ))

        # Insert this batch into SQLite
        conn.executemany("""
            INSERT INTO complaints
            VALUES (?, ?, ?, ?)
        """, records)

        conn.commit()
        total += len(records)

        if total % 100000 < len(records):
            print(f"Prepared {total} records")

    # Create an index for chronological reading
    conn.execute("""
        CREATE INDEX idx_created_date
        ON complaints(created_date, unique_key)
    """)
    conn.commit()

    print("Valid records:", total)
    print("Invalid dates:", invalid_dates)

    print("\nOldest five complaints:")

    cursor = conn.execute("""
        SELECT * FROM complaints
        ORDER BY created_date ASC, unique_key ASC
        LIMIT 5
    """)

    for row in cursor:
        print(row)

finally:
    conn.close()
