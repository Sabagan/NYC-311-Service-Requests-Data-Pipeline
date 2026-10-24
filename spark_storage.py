import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Writing local files with Spark on Windows needs winutils.exe + hadoop.dll.
# They live next to this script under winutils/bin.
if sys.platform == "win32":
    winutils_dir = Path(__file__).resolve().parent / "winutils"
    os.environ.setdefault("HADOOP_HOME", str(winutils_dir))
    os.environ["PATH"] = (
        str(winutils_dir / "bin") + os.pathsep + os.environ["PATH"]
    )

# Input and output file paths
csv_file = Path("311_Service_Requests_For_2026_Until_20261019.csv")
parquet_file = Path("311_Service_Requests_For_2026.parquet")

# Spark writes to a directory, so build via a temp dir and keep one file
temp_dir = Path("311_Service_Requests_For_2026_temp")

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