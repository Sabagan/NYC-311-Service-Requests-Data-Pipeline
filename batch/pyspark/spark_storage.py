import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

# Writing local files with Spark on Windows needs winutils.exe + hadoop.dll.
# They live in the project root under winutils/bin.
if sys.platform == "win32":
    winutils_dir = Path(__file__).resolve().parents[2] / "winutils"
    os.environ.setdefault("HADOOP_HOME", str(winutils_dir))
    os.environ["PATH"] = (
        str(winutils_dir / "bin") + os.pathsep + os.environ["PATH"]
    )

# Resolve the project root so these paths work from any working directory.
ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSING = ROOT / "data" / "processing"

# Input and output file paths
csv_file = (
    DATA_PROCESSING / "311_Service_Requests_For_2026_Until_20261019_clean.csv"
)
parquet_file = DATA_PROCESSING / "311_Service_Requests_For_2026.parquet"

# Month-partitioned layout: a directory of created_month=YYYY-MM/ subfolders.
partitioned_dir = (
    DATA_PROCESSING / "311_Service_Requests_For_2026_month_partitioned.parquet"
)

month_column = "created_month"
created_column = "Created Date"
timestamp_format = "MM/dd/yyyy hh:mm:ss a"

# Spark writes to a directory, so build via a temp dir in data/raw and keep one
# file in data/processing
temp_dir = DATA_RAW / "311_Service_Requests_For_2026_temp"


def dir_size_mb(path):
    """Total size of the files under a directory (a partitioned dataset)."""
    return sum(
        p.stat().st_size for p in path.rglob("*") if p.is_file()
    ) / 1_000_000


def partition_values(path):
    """Month values read from the created_month=YYYY-MM directory names."""
    return sorted(
        p.name.split("=", 1)[1] for p in path.iterdir() if p.is_dir()
    )


spark = (
    SparkSession.builder
    .appName("NYC311-CSV-To-Parquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    # Read CSV file (all columns as strings, same options as spark_analysis.py)
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        # Values quote-double their internal quotes; see spark_analysis.py.
        .option("escape", '"')
        .csv(str(csv_file))
    )

    # Derive the month partition from the canonical Created Date text. The
    # cleaning step guarantees the format; keep Spark's parser as a fallback so
    # a hand-edited value does not land in a null partition.
    enriched = raw.withColumn(
        month_column,
        F.date_format(
            F.coalesce(
                F.to_timestamp(F.col(created_column), timestamp_format),
                F.to_timestamp(F.col(created_column)),
            ),
            "yyyy-MM",
        ),
    )

    # Convert to Parquet with Snappy compression
    # Can update numPartitions as needed (1 = single output file)
    (
        raw.coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(temp_dir))
    )

    # Move the single part file to the final parquet path and clean up
    part_file = next(
        p for p in temp_dir.iterdir()
        if p.name.startswith("part-") and p.name.endswith(".parquet")
    )
    DATA_PROCESSING.mkdir(parents=True, exist_ok=True)
    shutil.move(str(part_file), str(parquet_file))
    shutil.rmtree(temp_dir)

    # Write the month-partitioned layout. Coalescing first keeps one file per
    # month instead of one per Spark partition, so the dataset stays at
    # created_month=YYYY-MM/part-*.parquet. A query that filters on
    # created_month (e.g. WHERE created_month = '2026-03') then reads only
    # that month's folder.
    (
        enriched.coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy(month_column)
        .parquet(str(partitioned_dir))
    )

    # Verify both layouts read back correctly with Spark
    df = spark.read.parquet(str(parquet_file))
    row_count = df.count()

    partitioned_df = spark.read.parquet(str(partitioned_dir))
    partitioned_row_count = partitioned_df.count()
    partitions = partition_values(partitioned_dir)

    assert partitioned_row_count == row_count, (
        f"Partitioned layout holds {partitioned_row_count:,} rows, "
        f"single file holds {row_count:,}"
    )

    # Get file sizes in MB
    csv_size_mb = csv_file.stat().st_size / 1_000_000
    parquet_size_mb = parquet_file.stat().st_size / 1_000_000
    partitioned_size_mb = dir_size_mb(partitioned_dir)

    # Calculate percentage reductions against the CSV
    reduction_pct = (
        (csv_size_mb - parquet_size_mb) / csv_size_mb * 100
    )
    partitioned_reduction_pct = (
        (csv_size_mb - partitioned_size_mb) / csv_size_mb * 100
    )

    # Report results
    print(f"Rows written:\t\t{row_count:,}")
    print(f"CSV file size:\t\t{csv_size_mb:.2f} MB")
    print(f"Parquet file size:\t{parquet_size_mb:.2f} MB")
    print(f"Storage reduction:\t{reduction_pct:.2f}%")

    month_range = (
        f"{partitions[0]} .. {partitions[-1]}" if partitions else "none"
    )

    print("\nMonth-partitioned Parquet:")
    print(f"Directory:\t\t{partitioned_dir.name}")
    print(f"Month partitions:\t{len(partitions)} ({month_range})")
    print(f"Rows written:\t\t{partitioned_row_count:,}")
    print(f"Parquet size:\t\t{partitioned_size_mb:.2f} MB")
    print(f"Storage reduction:\t{partitioned_reduction_pct:.2f}%")
    print(
        f"vs single file:\t\t"
        f"{(partitioned_size_mb - parquet_size_mb) / parquet_size_mb * 100:+.2f}%"
    )
finally:
    spark.stop()
