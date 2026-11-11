import statistics
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# Make src/ importable when this file is run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.spark_config import DATA_PROCESSING, configure_hadoop

# Reading a partitioned Parquet directory on Windows needs winutils.exe +
# hadoop.dll, because Hadoop's native IO does the directory listing.
configure_hadoop()

# Change path based on file name
CSV_PATH = str(
    DATA_PROCESSING / "311_Service_Requests_For_2026_Until_20261019_clean.csv"
)
PARQUET_PATH = str(DATA_PROCESSING / "311_Service_Requests_For_2026.parquet")

# Directory of created_month=YYYY-MM/ folders, written by convert_to_parquet.py.
PARTITIONED_PARQUET_PATH = str(
    DATA_PROCESSING / "311_Service_Requests_For_2026_month_partitioned.parquet"
)

REPEATS = 3

# Every layout the same queries run against: (label, loader format, path).
DATASETS = [
    ("CSV", "csv", CSV_PATH),
    ("Parquet", "parquet", PARQUET_PATH),
    (
        "Month-partitioned Parquet",
        "parquet_month_partitioned",
        PARTITIONED_PARQUET_PATH,
    ),
]

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
    elif file_format in ("parquet", "parquet_month_partitioned"):
        # The partitioned layout is read exactly like the single file; Spark
        # discovers created_month from the folder names, so a query that
        # filters on it reads only the matching months (partition pruning).
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


def benchmark(path, file_format, label, query_function):
    timings = []
    result = None

    for run in range(REPEATS):
        start = time.perf_counter()

        # Include DataFrame construction and the action.
        df = load_data(path, file_format)
        result = query_function(df).collect()

        elapsed = time.perf_counter() - start
        timings.append(elapsed)

        print(f"{label} | Run {run + 1}: {elapsed:.2f} seconds")

    return result, statistics.median(timings)


def normalize_result(rows):
    # Compare values independently of row ordering.
    return sorted(tuple(row) for row in rows)


missing_inputs = [path for _, _, path in DATASETS if not Path(path).exists()]

if missing_inputs:
    sys.exit(
        "Missing input(s):\n  "
        + "\n  ".join(missing_inputs)
        + "\nRun clean_requests.py and convert_to_parquet.py first."
    )

try:
    for query_name, query_function in [
        ("Complaint types by borough", question_1),
        ("Average closure time by agency", question_2),
    ]:
        print(f"\n===== {query_name} =====")

        results = {}
        medians = {}

        for label, file_format, path in DATASETS:
            result, median = benchmark(
                path, file_format, label, query_function
            )
            results[label] = result
            medians[label] = median

        for label in medians:
            print(f"{label} Results: ", results[label])

        csv_normalized = normalize_result(results["CSV"])

        for label in medians:
            print(
                f"{label} results match CSV:",
                normalize_result(results[label]) == csv_normalized
            )

        for label in medians:
            print(f"{label} median: {medians[label]:.2f} seconds")

        csv_time = medians["CSV"]

        for label in medians:
            if label != "CSV" and medians[label] > 0:
                print(
                    f"Speedup (CSV -> {label}): "
                    f"{csv_time / medians[label]:.2f}x"
                )

        if medians["Parquet"] > 0 and medians["Month-partitioned Parquet"] > 0:
            print(
                "Speedup (Parquet -> month-partitioned Parquet): "
                f"{medians['Parquet'] / medians['Month-partitioned Parquet']:.2f}x"
            )

finally:
    spark.stop()
