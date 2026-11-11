import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

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

# Spark writes to a directory, so build via a temp dir in data/raw and keep one
# file in data/processing
temp_dir = DATA_RAW / "311_Service_Requests_For_2026_temp"

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

    # Verify it reads back correctly with Spark
    df = spark.read.parquet(str(parquet_file))
    row_count = df.count()
    print(f"Rows written:\t\t{row_count}")

    # Get file size in MB
    csv_size_mb = csv_file.stat().st_size / 1_000_000
    parquet_size_mb = parquet_file.stat().st_size / 1_000_000

    # Calculate percentage reduction
    reduction_pct = (
        (csv_size_mb - parquet_size_mb) / csv_size_mb * 100
    )

    # Report results
    print(f"CSV file size:\t\t{csv_size_mb:.2f} MB")
    print(f"Parquet file size:\t{parquet_size_mb:.2f} MB")
    print(f"Storage reduction:\t{reduction_pct:.2f}%")
finally:
    spark.stop()