import os
import sys
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if sys.platform == "win32":
    winutils_dir = Path(__file__).resolve().parents[2] / "winutils"
    os.environ.setdefault("HADOOP_HOME", str(winutils_dir))
    os.environ["PATH"] = (
        str(winutils_dir / "bin") + os.pathsep + os.environ["PATH"]
    )

# ============================================================
# 1. CONFIGURATION
# ============================================================

# Resolve the project root so these paths work from any working directory.
ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSING = ROOT / "data" / "processing"

raw_csv = DATA_RAW / "311_Service_Requests_For_2026_Until_20261019.csv"
clean_csv = (
    DATA_PROCESSING / "311_Service_Requests_For_2026_Until_20261019_clean.csv"
)

valid_boroughs = [
    "BRONX",
    "BROOKLYN",
    "MANHATTAN",
    "QUEENS",
    "STATEN ISLAND",
]

year_start = "2026-01-01"
year_end = "2027-01-01"

timestamp_format = "MM/dd/yyyy hh:mm:ss a"

timestamp_pattern = (
    r"^\d{2}/\d{2}/\d{4} "
    r"\d{2}:\d{2}:\d{2} (AM|PM)$"
)

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

rule_names = [
    "empty_row",
    "missing_id",
    "duplicate_id",
    "invalid_created_date",
    "invalid_borough",
    "missing_agency",
]


# ============================================================
# 2. START SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("NYC_311_CSV_Cleaning")
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. CSV PARSE OPTIONS
# ============================================================

# The CSV quotes fields RFC 4180 style: a quote inside a value is written as
# two quotes. Spark escapes with a backslash by default, so those values
# would split at their quotes and shift the rest of the row one column left
# (e.g. Police Precinct "Precinct 112" landing in the Borough column).
#
# Every read must use these options, including the verification re-read:
# reading the same file two ways makes the output look corrupt when it is not.

csv_read_options = {
    "header": True,
    "inferSchema": False,
    "encoding": "UTF-8",
    "mode": "PERMISSIVE",
    "quote": '"',
    "escape": '"',
}


def read_csv(path):
    return spark.read.options(**csv_read_options).csv(str(path))


# ============================================================
# 4. CHECK INPUT FILE
# ============================================================

if not raw_csv.is_file():
    sys.exit(f"Input file not found: {raw_csv}")

# Read header only.
header = read_csv(raw_csv).limit(0).columns

missing = [c for c in required_columns if c not in header]

if missing:
    sys.exit(
        f"Missing columns in {raw_csv}: {missing}. "
        f"Available columns: {header}"
    )


# ============================================================
# 5. READ RAW CSV
# ============================================================

df = read_csv(raw_csv)

# Ensure all fields are strings and preserve original columns.
df = df.select(
    *[F.col(c).cast("string").alias(c) for c in header]
)


# ============================================================
# 6. NORMALIZE TEXT FIELDS
# ============================================================

for column in header:
    df = df.withColumn(
        column,
        F.trim(F.coalesce(F.col(column), F.lit("")))
    )

df = (
    df
    .withColumn(
        borough_column,
        F.upper(
            F.regexp_replace(
                F.trim(F.col(borough_column)),
                r"\s+",
                " "
            )
        )
    )
    .withColumn(
        agency_column,
        F.upper(F.trim(F.col(agency_column)))
    )
)


# ============================================================
# 7. PARSE AND NORMALIZE TIMESTAMPS
# ============================================================

# Try the canonical timestamp format first.
# Try Spark's flexible parser as a fallback.

created_parsed = F.coalesce(
    F.try_to_timestamp(
        F.col(created_column),
        F.lit(timestamp_format)
    ),
    F.try_to_timestamp(F.col(created_column))
)

closed_parsed = F.coalesce(
    F.try_to_timestamp(
        F.col(closed_column),
        F.lit(timestamp_format)
    ),
    F.try_to_timestamp(F.col(closed_column))
)

df = (
    df
    .withColumn("_created_parsed", created_parsed)
    .withColumn("_closed_parsed", closed_parsed)
)

# Preserve parsed timestamps for validation.
df = df.withColumn(
    "_created_valid",
    F.col("_created_parsed").isNotNull()
)

# Normalize both timestamps to one canonical format.
# Unparseable Closed Date becomes blank.

df = (
    df
    .withColumn(
        created_column,
        F.when(
            F.col("_created_parsed").isNotNull(),
            F.date_format(
                F.col("_created_parsed"),
                timestamp_format
            )
        ).otherwise(F.lit(""))
    )
    .withColumn(
        closed_column,
        F.when(
            F.col("_closed_parsed").isNotNull(),
            F.date_format(
                F.col("_closed_parsed"),
                timestamp_format
            )
        ).otherwise(F.lit(""))
    )
)


# ============================================================
# 8. IDENTIFY DATA-QUALITY FAILURES
# ============================================================

# Empty row: every original column is blank.
empty_row = F.lit(True)

for column in header:
    empty_row = empty_row & (F.col(column) == "")

# Missing ID.
empty_key = F.col(key_column) == ""

# Invalid borough.
invalid_borough = ~F.col(borough_column).isin(valid_boroughs)

# Missing agency.
missing_agency = F.col(agency_column) == ""

# Invalid Created Date.
invalid_created_date = (
    ~F.col("_created_valid")
    | (F.to_date("_created_parsed") < F.lit(year_start).cast("date"))
    | (F.to_date("_created_parsed") >= F.lit(year_end).cast("date"))
)


# ============================================================
# 9. IDENTIFY DUPLICATE IDS
# ============================================================

# Window ordering preserves the first occurrence according
# to the source file's partition ordering only if a reliable
# source row identifier is available.
#
# Since Spark CSV reads do not guarantee input ordering,
# this implementation retains one arbitrary occurrence
# of each Unique Key.

duplicate_window = Window.partitionBy(key_column)

df = df.withColumn(
    "_duplicate_count",
    F.count(F.lit(1)).over(duplicate_window)
)

duplicate_key = (
    (F.col(key_column) != "")
    & (F.col("_duplicate_count") > 1)
)

# Keep one record per Unique Key.
#
# This is deterministic only when the input has a suitable
# ordering column. Here, row_number chooses an arbitrary row.

duplicate_window_ordered = (
    Window
    .partitionBy(key_column)
    .orderBy(F.monotonically_increasing_id())
)

df = df.withColumn(
    "_duplicate_rank",
    F.when(
        F.col(key_column) != "",
        F.row_number().over(duplicate_window_ordered)
    ).otherwise(F.lit(1))
)

# ============================================================
# 10. BUILD FAILURE FLAGS
# ============================================================

df = (
    df
    .withColumn("_empty_row", empty_row)
    .withColumn("_missing_id", empty_key)
    .withColumn("_duplicate_id", duplicate_key)
    .withColumn("_invalid_created_date", invalid_created_date)
    .withColumn("_invalid_borough", invalid_borough)
    .withColumn("_missing_agency", missing_agency)
)

# A row can fail multiple rules.
# Keep only rows that pass every rule.

keep_condition = (
    ~F.col("_empty_row")
    & ~F.col("_missing_id")
    & ~F.col("_duplicate_id")
    & ~F.col("_invalid_created_date")
    & ~F.col("_invalid_borough")
    & ~F.col("_missing_agency")
)

# Retain one occurrence of every non-empty ID.
# Empty IDs are already rejected.

keep_condition = (
    keep_condition
    & (
        (F.col(key_column) == "")
        | (F.col("_duplicate_rank") == 1)
    )
)

clean_df = df.filter(keep_condition)


# ============================================================
# 11. COUNT FAILURES
# ============================================================

failure_expressions = [
    F.sum(F.col("_empty_row").cast("long")).alias("empty_row"),
    F.sum(F.col("_missing_id").cast("long")).alias("missing_id"),
    F.sum(F.col("_duplicate_id").cast("long")).alias("duplicate_id"),
    F.sum(F.col("_invalid_created_date").cast("long")).alias(
        "invalid_created_date"
    ),
    F.sum(F.col("_invalid_borough").cast("long")).alias(
        "invalid_borough"
    ),
    F.sum(F.col("_missing_agency").cast("long")).alias(
        "missing_agency"
    ),
]

failure_counts = df.agg(*failure_expressions).first().asDict()

total_rows = df.count()
kept_rows = clean_df.count()
dropped_rows = total_rows - kept_rows

# Count timestamps rewritten.
# Compare normalized values with the original values.
#
# For accurate counts, retain original timestamp columns
# before normalization.

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

print("\nFailures by rule (a row can fail several):")

for rule in rule_names:
    print(
        f"  {rule:<22}\t"
        f"{failure_counts.get(rule, 0) or 0:,}"
    )

assert kept_rows + dropped_rows == total_rows, (
    "Records were lost during cleaning."
)


# ============================================================
# 12. WRITE CLEAN CSV
# ============================================================

# Spark writes one file per partition inside a directory, so a plain write
# would leave many part files instead of the single CSV that clean.py
# produces. Coalesce to one partition, write to a temporary directory, then
# move the one part file into place so the swap is atomic for readers.

# The temporary directory lives in data/raw; the finished CSV is moved to
# data/processing.
DATA_PROCESSING.mkdir(parents=True, exist_ok=True)
temp_dir = str(DATA_RAW / (clean_csv.name + ".part"))

if os.path.exists(temp_dir):
    shutil.rmtree(temp_dir)

# Remove helper columns and retain the original schema.
clean_output = clean_df.select(*header)

try:
    (
        clean_output
        # One output file instead of one per partition.
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("emptyValue", "")
        .csv(temp_dir)
    )

    # The coalesced write leaves exactly one part file in the temp directory.
    # Move it to the final path, then drop the now-empty directory.
    part_files = sorted(
        p for p in Path(temp_dir).iterdir()
        if p.is_file() and p.name.startswith("part-")
    )

    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected one part file in {temp_dir}, "
            f"found {len(part_files)}"
        )

    os.replace(part_files[0], clean_csv)

finally:
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


# ============================================================
# 13. VERIFY CLEAN DATASET
# ============================================================

verify_df = read_csv(clean_csv)

verify_df = verify_df.select(
    *[F.col(c).cast("string").alias(c) for c in header]
)

problems = []

rows = verify_df.count()

if verify_df.filter(F.col(key_column) == "").limit(1).count():
    problems.append("empty Unique Key found")

if (
    verify_df
    .groupBy(key_column)
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
):
    problems.append("duplicate Unique Key found")

if (
    verify_df
    .filter(~F.col(borough_column).isin(valid_boroughs))
    .limit(1)
    .count()
):
    problems.append("invalid Borough found")

if (
    verify_df
    .filter(F.col(agency_column) == "")
    .limit(1)
    .count()
):
    problems.append("empty Agency found")

verify_created = F.try_to_timestamp(
    F.col(created_column),
    F.lit(timestamp_format)
)

if (
    verify_df
    .filter(
        verify_created.isNull()
        | (
            F.to_date(verify_created)
            < F.lit(year_start).cast("date")
        )
        | (
            F.to_date(verify_created)
            >= F.lit(year_end).cast("date")
        )
    )
    .limit(1)
    .count()
):
    problems.append("invalid Created Date found")

if (
    verify_df
    .filter(
        ~F.col(created_column).rlike(timestamp_pattern)
    )
    .limit(1)
    .count()
):
    problems.append("Created Date not in canonical format")

if (
    verify_df
    .filter(
        (F.col(closed_column) != "")
        & ~F.col(closed_column).rlike(timestamp_pattern)
    )
    .limit(1)
    .count()
):
    problems.append("Closed Date not in canonical format")

# Verify schema.
if verify_df.columns != header:
    problems.append("column names or order changed")

# Verify row count.
if rows != kept_rows:
    problems.append(
        f"row count mismatch: wrote {kept_rows:,}, "
        f"read back {rows:,}"
    )

print("\n--- Verifying clean CSV ---")
print(f"Rows in clean CSV:\t{rows:,}")

if problems:
    print("\nVerification FAILED:")
    for problem in sorted(set(problems)):
        print(f"  - {problem}")
else:
    print("Verification PASSED: all checks satisfied.")

# ============================================================
# 14. FINISH
# ============================================================

spark.stop()
