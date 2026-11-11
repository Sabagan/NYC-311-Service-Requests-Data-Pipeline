# NYC 311 Service Requests Data Pipeline

## Dataset
https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data

## License
NYC Open Data (no restrictions on use; free public download), Terms of Use: https://data.cityofnewyork.us/stories/s/Terms-of-Use/k9k7-3cje/

## User-Raised Questions
1. Which complaint types are most common in a borough? (batch)
2. How long does each agency take to close a request, on average? (batch)
3. How many noise complaints came in per hour in each ZIP code tonight? (stream)

## Directory Layout
- `batch/` - `spark_config.py` (shared paths and Windows Hadoop setup), `clean_requests.py`, `convert_to_parquet.py` and `benchmark_layouts.py` (PySpark implementations)
- `data/raw/` - the downloaded CSV and temporary build files
- `data/processing/` - the cleaned CSV, the single-file Parquet, and the month-partitioned Parquet directory produced by the batch scripts
- `streaming/` - the Kafka producer, consumer and replay preparation scripts

## Storage
- Download the CSV from Open Data NYC, containing all 2026 data until present data, then use `batch/convert_to_parquet.py` to convert it to Parquet

`batch/convert_to_parquet.py` also writes a month-partitioned Parquet dataset at `data/processing/311_Service_Requests_For_2026_month_partitioned.parquet/`, laid out as `created_month=YYYY-MM/part-*.parquet`. Spark discovers `created_month` from the folder names, so a query that filters on it reads only the matching months (partition pruning), and the size trade-off against the single-file Parquet is reported next to it.

## Spark-based Analysis
We will compare the performance of CSV vs Parquet vs month-partitioned Parquet in answering the first two (batch) questions, using batch/benchmark_layouts.py
