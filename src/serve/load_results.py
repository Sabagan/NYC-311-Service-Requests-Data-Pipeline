"""Part 3 (Serve): load the batch and stream results into PostgreSQL.

For each result CSV in output/, inside one transaction:

  1. check the CSV header has the expected columns (wrong schema = stop)
  2. copy every row, as plain text, into a temporary staging table
  3. run the quality checks and tag each bad row with the first check it fails
  4. move bad rows to rejected_rows and good rows to the final table
  5. count rows in and rows out, and stop if they do not add up

One transaction per dataset (ACID, Week 9): if anything fails part way, the
whole load for that dataset is undone and the previous table stays as it was.

Run from the project root:  python src/serve/load_results.py
"""

import csv
import sys
import time
from pathlib import Path

# Make src/ importable when this file is run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.spark_config import OUTPUT_DIR
from serve.db import connect

SCHEMA_FILE = Path(__file__).with_name("schema.sql")

BOROUGHS = "('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND')"
WHOLE_NUMBER = r"'^[0-9]+$'"
DECIMAL_NUMBER = r"'^[0-9]+(\.[0-9]+)?$'"
ISO_TIMESTAMP = r"'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}'"


def missing_any(columns):
    """SQL that is true when any of the columns is empty or NULL."""
    return " OR ".join(f"coalesce(trim({c}), '') = ''" for c in columns)


# One entry per result file. "checks" run in order; a row is tagged with the
# first check it fails, so later checks can safely convert text to numbers or
# timestamps (the earlier format checks already caught anything unreadable).
DATASETS = [
    {
        "table": "q1_complaints_by_borough",
        "csv": OUTPUT_DIR / "q1_complaints_by_borough.csv",
        "made_by": "src/batch/export_batch_results.py",
        "columns": ["borough", "complaint_type", "request_count"],
        "key": ["borough", "complaint_type"],
        "checks": [
            ("missing_value",
             missing_any(["borough", "complaint_type", "request_count"])),
            ("invalid_borough", f"borough NOT IN {BOROUGHS}"),
            ("invalid_count", f"request_count !~ {WHOLE_NUMBER}"),
            ("invalid_count", "request_count::bigint < 1"),
        ],
        "target": "borough, complaint_type, request_count",
        "select": "borough, complaint_type, request_count::bigint",
    },
    {
        "table": "q2_agency_close_time",
        "csv": OUTPUT_DIR / "q2_agency_close_time.csv",
        "made_by": "src/batch/export_batch_results.py",
        "columns": ["agency", "avg_close_hours", "closed_requests"],
        "key": ["agency"],
        "checks": [
            ("missing_value",
             missing_any(["agency", "avg_close_hours", "closed_requests"])),
            # A negative average fails this too: the pattern has no minus sign.
            ("invalid_hours", f"avg_close_hours !~ {DECIMAL_NUMBER}"),
            ("invalid_count", f"closed_requests !~ {WHOLE_NUMBER}"),
            ("invalid_count", "closed_requests::bigint < 1"),
        ],
        "target": "agency, avg_close_hours, closed_requests",
        "select": "agency, avg_close_hours::numeric, closed_requests::bigint",
    },
    {
        "table": "q3_hourly_noise",
        "csv": OUTPUT_DIR / "hourly_noise_full.csv",
        "made_by": "the streaming pipeline (docker compose up ...)",
        "columns": ["window_start", "window_end", "incident_zip", "noise_count"],
        "key": ["incident_zip", "window_start"],
        "checks": [
            ("missing_value",
             missing_any(["window_start", "window_end",
                          "incident_zip", "noise_count"])),
            ("invalid_zip", r"incident_zip !~ '^[0-9]{5}$'"),
            ("invalid_count", f"noise_count !~ {WHOLE_NUMBER}"),
            ("invalid_count", "noise_count::bigint < 1"),
            ("invalid_window",
             f"window_start !~ {ISO_TIMESTAMP} OR window_end !~ {ISO_TIMESTAMP}"),
            ("invalid_window",
             "window_end::timestamptz <> window_start::timestamptz + interval '1 hour'"),
        ],
        "target": "incident_zip, window_start, window_end, noise_count",
        "select": ("incident_zip, window_start::timestamptz, "
                   "window_end::timestamptz, noise_count::bigint"),
    },
]


# ----------------------------------------------------------------- SQL text
# The table and column names below come from DATASETS above, never from user
# input, so building the SQL with f-strings is safe here.

def create_staging_sql(ds):
    columns = ", ".join(f"{c} TEXT" for c in ds["columns"])
    return (f"CREATE TEMP TABLE staging ({columns}, reason TEXT) "
            "ON COMMIT DROP")


def copy_sql(ds):
    return (f"COPY staging ({', '.join(ds['columns'])}) "
            "FROM STDIN WITH (FORMAT csv, HEADER true)")


def tag_bad_rows_sql(ds):
    whens = "\n".join(f"    WHEN {condition} THEN '{reason}'"
                      for reason, condition in ds["checks"])
    return f"UPDATE staging SET reason = CASE\n{whens}\nEND"


def tag_duplicates_sql(ds):
    # Rows that passed every other check but repeat the key of an earlier row.
    key = ", ".join(ds["key"])
    return f"""
        UPDATE staging AS s SET reason = 'duplicate_key'
        FROM (SELECT ctid AS row_id,
                     row_number() OVER (PARTITION BY {key} ORDER BY ctid) AS n
              FROM staging WHERE reason IS NULL) AS d
        WHERE s.ctid = d.row_id AND d.n > 1"""


def move_rejected_sql(ds):
    return f"""
        INSERT INTO rejected_rows (dataset, reason, raw_row)
        SELECT '{ds['table']}', reason, to_jsonb(s) - 'reason'
        FROM staging AS s WHERE reason IS NOT NULL"""


def insert_good_sql(ds):
    return f"""
        INSERT INTO {ds['table']} ({ds['target']})
        SELECT {ds['select']} FROM staging WHERE reason IS NULL"""


# ------------------------------------------------------------------ loading

def count_csv_rows(path):
    """Data rows in the file, read with Python's csv module (header excluded)."""
    with path.open(newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def read_header(path):
    with path.open(newline="", encoding="utf-8-sig") as f:
        return next(csv.reader(f), [])


def load_dataset(conn, ds):
    path = ds["csv"]
    header = read_header(path)
    if header != ds["columns"]:
        raise ValueError(f"{path.name}: wrong schema. Expected columns "
                         f"{ds['columns']}, found {header}")

    rows_in_file = count_csv_rows(path)
    start = time.perf_counter()

    with conn.transaction(), conn.cursor() as cur:
        cur.execute(create_staging_sql(ds))
        with cur.copy(copy_sql(ds)) as copy, path.open("rb") as f:
            while chunk := f.read(1 << 20):
                copy.write(chunk)

        cur.execute("SELECT count(*) FROM staging")
        rows_staged = cur.fetchone()[0]

        cur.execute(tag_bad_rows_sql(ds))
        cur.execute(tag_duplicates_sql(ds))
        cur.execute("SELECT reason, count(*) FROM staging "
                    "WHERE reason IS NOT NULL GROUP BY reason ORDER BY reason")
        failures = dict(cur.fetchall())

        # Replace the previous load of this dataset.
        cur.execute(f"TRUNCATE {ds['table']}")
        cur.execute("DELETE FROM rejected_rows WHERE dataset = %s",
                    (ds["table"],))
        cur.execute(move_rejected_sql(ds))
        rows_rejected = cur.rowcount
        cur.execute(insert_good_sql(ds))
        rows_loaded = cur.rowcount

        # Rows in must equal rows out; otherwise undo the whole load.
        if not (rows_in_file == rows_staged == rows_loaded + rows_rejected):
            raise RuntimeError(
                f"{ds['table']}: row counts do not add up: {rows_in_file} in "
                f"file, {rows_staged} staged, {rows_loaded} loaded, "
                f"{rows_rejected} rejected")

        # Refresh the planner's statistics so it picks good query plans.
        cur.execute(f"ANALYZE {ds['table']}")

    return {
        "rows_in": rows_in_file,
        "loaded": rows_loaded,
        "rejected": rows_rejected,
        "failures": failures,
        "seconds": time.perf_counter() - start,
    }


def main():
    with connect() as conn:
        with conn.transaction():
            conn.execute(SCHEMA_FILE.read_text(encoding="utf-8"))

        results = {}
        for ds in DATASETS:
            if not ds["csv"].is_file():
                print(f"SKIPPED {ds['table']}: {ds['csv'].name} not found "
                      f"in output/. It is made by {ds['made_by']}.")
                continue
            print(f"Loading {ds['csv'].name} into {ds['table']} ...")
            results[ds["table"]] = load_dataset(conn, ds)

    if not results:
        sys.exit("Nothing loaded: no result files found in output/.")

    print("\n--- Part 3 load report ---")
    print(f"{'table':<26}{'rows in':>10}{'loaded':>10}{'rejected':>10}"
          f"{'seconds':>9}")
    for table, r in results.items():
        print(f"{table:<26}{r['rows_in']:>10,}{r['loaded']:>10,}"
              f"{r['rejected']:>10,}{r['seconds']:>9.2f}")

    print("\nRows failing each check (first failed check per row):")
    for table, r in results.items():
        if r["failures"]:
            for reason, count in r["failures"].items():
                print(f"  {table:<26}{reason:<18}{count:>8,}")
        else:
            print(f"  {table:<26}{'none':<18}{0:>8}")
    print("\nRejected rows are kept in the rejected_rows table.")


if __name__ == "__main__":
    main()
