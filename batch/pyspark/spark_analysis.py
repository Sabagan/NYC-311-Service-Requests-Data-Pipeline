import time
import statistics
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Resolve the project root so these paths work from any working directory.
ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSING = ROOT / "data" / "processing"

# Change path based on file name
CSV_PATH = str(
    DATA_PROCESSING / "311_Service_Requests_For_2026_Until_20261019_clean.csv"
)
PARQUET_PATH = str(DATA_PROCESSING / "311_Service_Requests_For_2026.parquet")
REPEATS = 3

spark = (
    SparkSession.builder
    .appName("NYC311-CSV-Parquet-Comparison")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def load_data(path, file_format):
    if file_format == "csv":
        raw = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("mode", "PERMISSIVE")
            # The CSV quotes fields RFC 4180 style: a quote inside a value is
            # doubled (""...""). Spark escapes with a backslash by default,
            # so those values would split at their commas and shift the rest
            # of the row one column left (e.g. Police Precinct becoming
            # Borough). Unescape with a quote instead.
            .option("escape", '"')
            .csv(path)
        )
    elif file_format == "parquet":
        raw = spark.read.parquet(path)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

    # Normalize both inputs to a consistent schema.
    required = [
        "Borough",
        "Problem (formerly Complaint Type)",
        "Agency",
        "Created Date",
        "Closed Date",
    ]

    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise ValueError(
            f"Missing columns in {path}: {missing}. "
            f"Available columns: {raw.columns}"
        )

    return raw.select(
        F.trim(F.col("Borough").cast("string")).alias("borough"),
        F.trim(
            F.col("Problem (formerly Complaint Type)").cast("string")
        ).alias("complaint_type"),
        F.trim(F.col("Agency").cast("string")).alias("agency"),

        # NYC 311 dates commonly use MM/dd/yyyy hh:mm:ss AM/PM.
        F.coalesce(
            F.to_timestamp("Created Date", "MM/dd/yyyy hh:mm:ss a"),
            F.to_timestamp("Created Date")
        ).alias("created_ts"),

        F.coalesce(
            F.to_timestamp("Closed Date", "MM/dd/yyyy hh:mm:ss a"),
            F.to_timestamp("Closed Date")
        ).alias("closed_ts"),
    )


def question_1(df):
    counts = (
        df.filter(
            F.col("borough").isNotNull()
            & (F.col("borough") != "")
            & F.col("complaint_type").isNotNull()
            & (F.col("complaint_type") != "")
        )
        .groupBy("borough", "complaint_type")
        .count()
    )

    window = Window.partitionBy("borough").orderBy(
        F.desc("count"),
        F.asc("complaint_type")
    )

    return (
        counts
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") <= 5)
        .orderBy("borough", "rank")
    )


def question_2(df):
    valid = df.filter(
        F.col("agency").isNotNull()
        & (F.col("agency") != "")
        & F.col("created_ts").isNotNull()
        & F.col("closed_ts").isNotNull()
        & (F.col("closed_ts") >= F.col("created_ts"))
    )

    return (
        valid
        .withColumn(
            "closure_hours",
            (
                F.col("closed_ts").cast("long")
                - F.col("created_ts").cast("long")
            ) / 3600.0
        )
        .groupBy("agency")
        .agg(
            F.round(F.avg("closure_hours"), 2)
                .alias("avg_closure_hours"),
            F.count("*").alias("closed_request_count")
        )
        .orderBy("agency")
    )


def benchmark(path, file_format, query_function):
    timings = []
    result = None

    for run in range(REPEATS):
        start = time.perf_counter()

        # Include DataFrame construction and the action.
        df = load_data(path, file_format)
        result = query_function(df).collect()

        elapsed = time.perf_counter() - start
        timings.append(elapsed)

        print(
            f"{file_format.upper()} | Run {run + 1}: "
            f"{elapsed:.2f} seconds"
            # f" - {result}"
        )

    return result, statistics.median(timings)


def normalize_result(rows):
    # Compare values independently of row ordering.
    return sorted(tuple(row) for row in rows)


try:
    for query_name, query_function in [
        ("Complaint types by borough", question_1),
        ("Average closure time by agency", question_2),
    ]:
        print(f"\n===== {query_name} =====")

        csv_result, csv_time = benchmark(
            CSV_PATH, "csv", query_function
        )

        parquet_result, parquet_time = benchmark(
            PARQUET_PATH, "parquet", query_function
        )

        print("CSV Results: ", csv_result)
        print("Parquet Results: ", parquet_result)
        
        print(
            "Results match:",
            normalize_result(csv_result)
            == normalize_result(parquet_result)
        )
        print(f"CSV median: {csv_time:.2f} seconds")
        print(f"Parquet median: {parquet_time:.2f} seconds")

        if parquet_time > 0:
            print(f"Speedup: {csv_time / parquet_time:.2f}x")

finally:
    spark.stop()
