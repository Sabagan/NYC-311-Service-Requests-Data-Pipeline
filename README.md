# NYC 311 Service Requests Data Pipeline

## Dataset

[NYC 311 Service Requests from 2020 to Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data)

## License

NYC Open Data (no restrictions on use; free public download). [Terms of Use](https://data.cityofnewyork.us/stories/s/Terms-of-Use/k9k7-3cje/)

## User-Raised Questions

1. Which complaint types are most common in a borough? (batch)
2. How long does each agency take to close a request, on average? (batch)
3. How many noise complaints came in per hour in each ZIP code tonight? (stream)

## Directory Layout

- `src/batch/` – `clean_requests.py`, `convert_to_parquet.py`, and `benchmark_layouts.py` (PySpark implementations), plus their run output.
- `src/config/` – `spark_config.py`, shared project paths, and Windows Hadoop setup.
- `src/stream/` – `prepare_replay.py`, `producer.py`, and `consumer.py`, plus `streaming_analysis_output.txt` containing streaming results and verification. Local SQLite state and Spark checkpoints are generated here.
- `bin/` – `winutils.exe` and `hadoop.dll`, the Windows Hadoop files used by Spark.
- `data/raw/` – the downloaded CSV and temporary build files.
- `data/processing/` – cleaned CSV, single-file Parquet, and month-partitioned Parquet directory produced by the batch scripts.
- `output/` – final streaming output, `hourly_noise_full.csv` (when generated or shared with the repository).
- `compose.yaml` – Docker Compose configuration for Kafka.
- `requirements.txt` – Python dependencies.

## Storage

Download the NYC Open Data CSV containing the 2026 records, then use `src/batch/convert_to_parquet.py` to convert it to Parquet.

`src/batch/convert_to_parquet.py` also writes a month-partitioned Parquet dataset at `data/processing/311_Service_Requests_For_2026_month_partitioned.parquet/`, laid out as `created_month=YYYY-MM/part-*.parquet`. Spark discovers `created_month` from the folder names, so a query that filters on it reads only the matching months (partition pruning). The size trade-off against the single-file Parquet is reported next to it.

## Requirements

- Python 3.12 and packages listed in `requirements.txt`.
- Java 21 for the tested PySpark environment.
- Docker Desktop with Docker Compose, for Kafka.
- On Windows, Hadoop support files in the project's `bin/`. The shared configuration points `HADOOP_HOME` at the project root and adds `bin/` to `PATH` before Python starts Spark.

All script data and output paths are resolved from the source file locations, not the terminal's working directory. Batch and streaming scripts share `src/config/spark_config.py`. Keep `bin/winutils.exe` and `bin/hadoop.dll` with the project when moving it.

Install the Python packages from the project root:

```powershell
python -m pip install -r requirements.txt
```

If other batch or PostgreSQL scripts require additional Python packages, add them to `requirements.txt` before final team submission.

## Spark-based Analysis (Batch: Q1 and Q2)

The batch scripts in `src/batch/` support comparison of CSV, single-file Parquet, and month-partitioned Parquet while answering the first two questions. See the scripts and their run output for the benchmark results.

## Kafka + Spark Structured Streaming (Q3)

The streaming pipeline replays historical NYC 311 complaints to Kafka and uses Spark Structured Streaming to count noise complaints for each ZIP code and one-hour event-time window.

### Input

`src/stream/prepare_replay.py` expects the Parquet input at:

```
data/processing/311_Service_Requests_For_2026.parquet
```

This dataset file may be excluded from Git because of its size. Prepare it using the batch processing scripts or place an equivalent dataset at this path before running the replay.

### Run with Docker (Windows, macOS, or Linux)

Start Docker Desktop, place the input Parquet at the path above, and run from the repository root:

```powershell
docker compose up --build --abort-on-container-exit --exit-code-from streaming
```

Compose waits for Kafka to become healthy. The streaming container prepares SQLite,
produces all records, then runs the Spark consumer and exports `output/hourly_noise_full.csv`.
The command stops Kafka when streaming finishes and returns the streaming exit code.
The first run downloads the image, Python dependencies, and Spark Kafka connector.
No host Python, Java, or Windows Hadoop configuration is needed for this Docker run.

Each invocation creates a new topic and isolated SQLite/checkpoint directory in the
`stream-runs` volume. This is a fresh full replay, not a resume. The final CSV is
replaced on success. Previous run state and Kafka topics remain in Docker volumes.
Do not run multiple copies concurrently because they share the final CSV path.

To remove containers while retaining run data: `docker compose down`.
To discard all Docker replay state and Kafka data: `docker compose down -v`.
The latter does not delete your mounted Parquet input or output CSV.

The Docker image uses Linux Hadoop support; `bin/` remains the shared Windows
Hadoop setup for running scripts directly on Windows. Compose Kafka now advertises
its internal service address, so this configuration is intended for the container pipeline.

### Output

`output/hourly_noise_full.csv` contains:

| Column | Meaning |
| --- | --- |
| `window_start` | Start of the one-hour event-time window (UTC in the exported CSV) |
| `window_end` | End of the window (exclusive; UTC in the exported CSV) |
| `incident_zip` | ZIP code associated with the complaints |
| `noise_count` | Number of eligible noise complaint records for that ZIP-hour window |

Each row describes one ZIP code during one hour. An absent ZIP-hour combination has no output row; it is not stored as a row with a count of zero.

### Verified Full-Run Results

| Metric | Observed result |
| --- | ---: |
| Total NYC 311 records replayed | 3,171,200 |
| Kafka topic ending offset (partition 0) | 3,171,200 |
| Expected noise complaints with nonempty ZIP codes | 615,086 |
| Total `noise_count` from Spark | 615,086 |
| Unique ZIP-hour groups | 321,010 |
| Count difference | 0 |

The expected count came from the sorted source records, and the actual count was calculated by summing `noise_count` in the results. These totals matched. Matching totals alone does not prove that every individual ZIP-hour count is correct.

See `src/stream/streaming_analysis_output.txt` for the full-run summary.

## Serving Layer: PostgreSQL (Part 3)

The batch results (Q1, Q2) and the streaming results (Q3) are loaded into one PostgreSQL 17 database, which answers all three questions and measures each response time.

### Run with Docker

Start Docker Desktop, then from the repository root:

```powershell
docker compose run --rm --build serve
```

This starts the `postgres` container, waits until it is healthy, then runs three scripts in a `serve` container:

1. `src/batch/export_batch_results.py` runs Q1 and Q2 with Spark on `data/processing/311_Service_Requests_For_2026.parquet` and saves the answers to `output/q1_complaints_by_borough.csv` and `output/q2_agency_close_time.csv`. It is skipped, with a message, if the Parquet file is missing.
2. `src/serve/load_results.py` loads every result CSV found in `output/` into PostgreSQL, with quality checks and row counts.
3. `src/serve/query_results.py` answers the three questions and prints each answer with its response time.

To ask about a different borough, ZIP code or day:

```powershell
docker compose run --rm --build serve python -u src/serve/query_results.py --borough QUEENS --zip 11226 --date 2026-08-23 --explain
```

`--explain` also prints each query plan, which shows whether PostgreSQL used an index. The database keeps its data in the `pg-data` volume between runs; `docker compose down -v` deletes it.

### Tables

| Table | Answers | Primary key (index) |
| --- | --- | --- |
| `q1_complaints_by_borough` | Q1 | `(borough, complaint_type)` |
| `q2_agency_close_time` | Q2 | `(agency)` |
| `q3_hourly_noise` | Q3 | `(incident_zip, window_start)` |
| `rejected_rows` | Rows that failed a check, with the check name and the original row | `id` |

Each primary key matches how its question asks (by borough; by agency; by ZIP code and then a time range), so PostgreSQL finds the rows through the key's index instead of reading the whole table. The table definitions are in `src/serve/schema.sql`.

### Quality checks while loading

Each CSV is loaded inside one transaction: if anything fails, nothing is changed. The loader:

- stops if the CSV header does not have the expected columns (wrong schema);
- tags each row with the first check it fails: `missing_value`, `invalid_borough`, `invalid_count` (not a whole number, or below 1), `invalid_hours`, `invalid_zip` (not 5 digits), `invalid_window` (unreadable, or not exactly one hour), then `duplicate_key`;
- moves failing rows to `rejected_rows` instead of dropping them;
- checks that rows in the file = rows loaded + rows rejected, and undoes the load if not.

It prints a report of rows in, loaded and rejected per table, and how many rows failed each check.
