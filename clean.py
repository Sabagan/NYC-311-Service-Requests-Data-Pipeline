import csv
import os
import sys
from pathlib import Path

import pandas as pd

raw_csv = Path("311_Service_Requests_For_2026_Until_20261019.csv")
clean_csv = Path("311_Service_Requests_For_2026_Until_20261019_clean.csv")

chunk_size = 250_000

valid_boroughs = {
    "BRONX",
    "BROOKLYN",
    "MANHATTAN",
    "QUEENS",
    "STATEN ISLAND",
}

year_start = pd.Timestamp("2026-01-01")
year_end = pd.Timestamp("2027-01-01")

# One format for everything, so every downstream script reads the same thing.
timestamp_format = "%m/%d/%Y %I:%M:%S %p"
timestamp_pattern = r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} (AM|PM)$"

key_column = "Unique Key"
created_column = "Created Date"
closed_column = "Closed Date"
borough_column = "Borough"
agency_column = "Agency"

required_columns = [
    key_column,
    created_column,
    closed_column,
    borough_column,
    agency_column,
]

# Reasons a row is dropped, in report order.
rule_names = [
    "empty_row",
    "missing_id",
    "duplicate_id",
    "invalid_created_date",
    "invalid_borough",
    "missing_agency",
]


def parse_timestamps(values):
    """Parse NYC 311 timestamps to datetimes, NaT when unparseable."""
    parsed = pd.to_datetime(
        values, format=timestamp_format, errors="coerce"
    )

    unparsed = parsed.isna() & values.ne("")
    if unparsed.any():
        try:
            parsed.loc[unparsed] = pd.to_datetime(
                values[unparsed], errors="coerce", format="mixed"
            )
        except (TypeError, ValueError):
            parsed.loc[unparsed] = pd.to_datetime(
                values[unparsed], errors="coerce"
            )

    return parsed


def clean_chunk(df, seen_keys):
    """Clean one chunk and return (clean_df, failure_counts, dates_normalized)."""
    # 1. Normalize text fields.
    for column in df.columns:
        df[column] = df[column].str.strip()
    df[borough_column] = df[borough_column].str.upper()
    df[agency_column] = df[agency_column].str.upper()

    # 2. Parse the timestamps and write them back in one canonical format.
    created_text = df[created_column]
    closed_text = df[closed_column]

    created = parse_timestamps(created_text)
    closed = parse_timestamps(closed_text)

    created_out = created.dt.strftime(timestamp_format).fillna("")
    # An unparseable Closed Date becomes blank (= not closed yet).
    closed_out = closed.dt.strftime(timestamp_format).fillna("")

    df[created_column] = created_out
    df[closed_column] = closed_out

    # 3. Data-quality checks. True means the row violates the rule.
    empty_row = df.eq("").all(axis=1)

    empty_key = df[key_column].eq("")

    duplicate_key = ~empty_key & (
        df[key_column].duplicated(keep="first")
        | df[key_column].isin(seen_keys)
    )
    # Remember every key of this chunk so later duplicates are caught too.
    seen_keys.update(df.loc[~empty_key, key_column].tolist())

    invalid_created_date = (
        created.isna()
        | (created < year_start)
        | (created >= year_end)
    )

    invalid_borough = ~df[borough_column].isin(valid_boroughs)

    missing_agency = df[agency_column].eq("")

    failures = {
        "empty_row": empty_row,
        "missing_id": empty_key,
        "duplicate_id": duplicate_key,
        "invalid_created_date": invalid_created_date,
        "invalid_borough": invalid_borough,
        "missing_agency": missing_agency,
    }

    # 4. Keep the rows that pass every rule.
    keep = ~pd.concat(failures, axis=1).any(axis=1)
    clean = df.loc[keep].copy()

    # 5. Report how many timestamps needed re-writing.
    written = created_out[keep]
    written_closed = closed_out[keep]
    dates_normalized = int(
        (written != created_text[keep]).sum()
        + (written_closed != closed_text[keep]).sum()
    )

    return clean, failures, dates_normalized


def verify(path):
    """Re-read the clean CSV and prove the output actually satisfies the rules."""
    problems = []
    rows = 0
    keys = set()

    reader = pd.read_csv(
        path,
        usecols=required_columns,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        chunksize=chunk_size,
        encoding="utf-8-sig",
    )

    for chunk in reader:
        for column in chunk.columns:
            chunk[column] = chunk[column].str.strip()

        rows += len(chunk)

        if chunk[key_column].eq("").any():
            problems.append("empty Unique Key found")
        if chunk[key_column].duplicated().any() or chunk[key_column].isin(keys).any():
            problems.append("duplicate Unique Key found")
        keys.update(chunk[key_column].tolist())

        if ~chunk[borough_column].isin(valid_boroughs).all():
            problems.append("invalid Borough found")

        if chunk[agency_column].eq("").any():
            problems.append("empty Agency found")

        created = parse_timestamps(chunk[created_column])
        if (
            created.isna().any()
            or (created < year_start).any()
            or (created >= year_end).any()
        ):
            problems.append("invalid Created Date found")

        # Spark's to_timestamp() raises on any other layout, so both
        # timestamp columns must be canonical (empty Closed Date is allowed).
        if not chunk[created_column].str.match(timestamp_pattern).all():
            problems.append("Created Date not in MM/dd/yyyy hh:mm:ss AM|PM")
        closed = chunk[closed_column]
        if not closed.loc[closed.ne("")].str.match(timestamp_pattern).all():
            problems.append("Closed Date not in MM/dd/yyyy hh:mm:ss AM|PM")

    return rows, sorted(set(problems))


if not raw_csv.is_file():
    sys.exit(f"Input file not found: {raw_csv}")

# Check the schema once, on the header only.
header = pd.read_csv(raw_csv, nrows=0, encoding="utf-8-sig").columns
missing = [c for c in required_columns if c not in header]
if missing:
    sys.exit(
        f"Missing columns in {raw_csv}: {missing}. "
        f"Available columns: {list(header)}"
    )

reader = pd.read_csv(
    raw_csv,
    dtype=str,
    keep_default_na=False,
    na_filter=False,
    chunksize=chunk_size,
    encoding="utf-8-sig",
)

seen_keys = set()
rule_totals = dict.fromkeys(rule_names, 0)
total_rows = 0
kept_rows = 0
dates_normalized = 0

# Write to a temp file, then swap it in, so an interrupted run never
# leaves a half-written CSV that looks valid.
temp_csv = clean_csv.with_name(clean_csv.name + ".part")

try:
    with open(temp_csv, "w", encoding="utf-8", newline="") as handle:
        header_written = False

        for number, chunk in enumerate(reader, start=1):
            clean, failures, normalized = clean_chunk(chunk, seen_keys)

            total_rows += len(chunk)
            kept_rows += len(clean)
            dates_normalized += normalized
            for rule in rule_names:
                rule_totals[rule] += int(failures[rule].sum())

            if not clean.empty:
                clean.to_csv(
                    handle,
                    index=False,
                    header=not header_written,
                    quoting=csv.QUOTE_MINIMAL,
                    lineterminator="\n",
                )
                header_written = True

            print(
                f"Chunk {number}: read {total_rows:,} rows, "
                f"kept {kept_rows:,}, dropped {total_rows - kept_rows:,}"
            )

    if not header_written:
        sys.exit(f"No rows survived cleaning; {clean_csv} was not created.")

    os.replace(temp_csv, clean_csv)
finally:
    if temp_csv.exists():
        temp_csv.unlink()

dropped_rows = total_rows - kept_rows

print("\n--- Cleanup report ---")
print(f"Raw CSV:\t\t{raw_csv}")
print(f"Clean CSV:\t\t{clean_csv}")
print(f"Rows read:\t\t{total_rows:,}")
print(f"Rows dropped:\t\t{dropped_rows:,}")
print(f"Rows kept:\t\t{kept_rows:,}")
print(
    f"Rows kept %:\t\t"
    f"{(kept_rows / total_rows * 100) if total_rows else 0:.2f}%"
)
print(f"Timestamps rewritten:\t{dates_normalized:,}")

print("\nFailures by rule (a row can fail several):")
for rule in rule_names:
    print(f"  {rule:<22}\t{rule_totals[rule]:,}")

# Same reconciliation check as the ETL.
assert kept_rows + dropped_rows == total_rows, (
    "Records were lost during cleaning."
)

original_size_mb = raw_csv.stat().st_size / 1_000_000
clean_size_mb = clean_csv.stat().st_size / 1_000_000
print(
    f"\nCSV size:\t\t{original_size_mb:,.2f} MB -> "
    f"{clean_size_mb:,.2f} MB"
)

print("\n--- Verifying clean CSV ---")
rows, problems = verify(clean_csv)

# The clean file must have the exact same columns as the raw file.
clean_header = pd.read_csv(clean_csv, nrows=0, encoding="utf-8-sig").columns
if list(clean_header) != list(header):
    problems.append("column names or order changed")

if rows != kept_rows:
    problems.append(
        f"row count mismatch: wrote {kept_rows:,}, read back {rows:,}"
    )

print(f"Rows in clean CSV:\t{rows:,}")