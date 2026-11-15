import os

from kafka import KafkaProducer
from pathlib import Path
import sqlite3
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.spark_config import STREAM_DIR

DB_PATH = STREAM_DIR / "events_sorted.sqlite"
if not DB_PATH.is_file():
    raise FileNotFoundError(f"Replay database not found: {DB_PATH}. Run prepare_replay.py first.")

TOPIC = os.environ.get("KAFKA_TOPIC", "nyc311-replay-full")
MAX_EVENTS = None

producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    acks="all",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

conn = sqlite3.connect(DB_PATH)

cursor = conn.execute("""
    SELECT created_date, unique_key, incident_zip, complaint_type
    FROM complaints
    ORDER BY created_date ASC, unique_key ASC
""")

sent = 0
pending = []

try:
    for created_date, unique_key, incident_zip, complaint_type in cursor:
        if MAX_EVENTS and sent >= MAX_EVENTS:
            break

        complaint = {
            "unique_key": unique_key,
            "created_date": created_date,
            "incident_zip": incident_zip,
            "complaint_type": complaint_type
        }

        future = producer.send(
            TOPIC,
            key=(incident_zip or "unknown").encode("utf-8"),
            value=complaint
        )

        pending.append(future)
        sent += 1

        # Check acknowledgements in batches instead of
        # waiting after every single send
        if len(pending) >= 500:
            for f in pending:
                f.get(timeout=30)
            pending.clear()

            if sent % 100000 == 0:
                print(f"Sent {sent} complaints")

    # Wait for the remaining messages
    for f in pending:
        f.get(timeout=30)

    producer.flush()
    print(f"Finished! Sent {sent} complaints to Kafka.")

finally:
    producer.close()
    conn.close()
