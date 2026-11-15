# NYC 311 Service Requests Data Pipeline

A PySpark batch pipeline (Part 1) and a Kafka + Spark Structured Streaming
pipeline (Part 2) over NYC 311 service requests.

## Quick Start: Batch Pipeline (Part 1, Docker)

Part 1 (cleaning, Parquet conversion, and the layout benchmark) runs entirely
in Docker Desktop. The 2026 CSV ships with the project in `data/raw/` (it is too
large for Git), so from a fresh copy of the repository, with Docker Desktop
running:

```bash
docker compose build batch
docker compose run --rm batch
```

The container then runs the three batch scripts in order:

1. `src/batch/clean_requests.py` – validates the rows and writes one clean CSV.
2. `src/batch/convert_to_parquet.py` – writes the single-file and the
   month-partitioned Parquet datasets.
3. `src/batch/benchmark_layouts.py` – answers Q1 and Q2 against CSV, Parquet,
   and month-partitioned Parquet, and prints the timings, the correctness
   comparison, and the speedups.

Every run repeats all three steps over the raw CSV (~3.2M rows), so allow
15–60 minutes depending on the machine. The repository directory is mounted into
the container, so everything the scripts write lands on the host in `data/`
exactly as it does for a local run.

Give Docker Desktop at least 8 GB of memory: the Spark driver is configured for
4 GB in the image.

Run a single step, for example only the benchmark:

```bash
docker compose run --rm batch python src/batch/benchmark_layouts.py
```

### Input dataset

The batch scripts read this file:

```text
data/raw/311_Service_Requests_For_2026_Until_20261019.csv
```

It is the NYC Open Data CSV of 2026 service requests created from 2026-01-01 to
2026-10-19 (~2.8 GB) and it is included in this copy of the project under
`data/raw/`. If it is missing, download the CSV from the dataset link below and
save it under exactly that name; the pipeline stops with an `Input file not
found` message otherwise.

## Dataset

[NYC 311 Service Requests from 2020 to Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data)

The pipeline uses records created between 2026-01-01 and 2026-10-19 from that
dataset. The portal's CSV download for that date range is the file kept at
`data/raw/311_Service_Requests_For_2026_Until_20261019.csv`, with the portal's
human-readable column names, which is what the batch scripts expect.

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
- `bin/` – `winutils.exe` and `hadoop.dll`, the Windows Hadoop files used by Spark on Windows only.
- `data/raw/` – the original CSV and temporary build files.
- `data/processing/` – cleaned CSV, single-file Parquet, and month-partitioned Parquet directory produced by the batch scripts.
- `output/` – final streaming output, `hourly_noise_full.csv` (when generated or shared with the repository).
- `compose.yaml` – Docker Compose configuration: Kafka, and the `batch` service.
- `Dockerfile` – Python 3.12 + Java 17 + PySpark image for the batch pipeline.
- `requirements.txt` – Python dependencies.

## Storage

`src/batch/clean_requests.py` cleans the raw CSV from `data/raw/`, and
`src/batch/convert_to_parquet.py` converts the cleaned CSV to Parquet.

`src/batch/convert_to_parquet.py` also writes a month-partitioned Parquet dataset at `data/processing/311_Service_Requests_For_2026_month_partitioned.parquet/`, laid out as `created_month=YYYY-MM/part-*.parquet`. Spark discovers `created_month` from the folder names, so a query that filters on it reads only the matching months (partition pruning). The size trade-off against the single-file Parquet is reported next to it.

## Requirements

For the Docker path (recommended): Docker Desktop with Docker Compose and at
least 8 GB of memory allocated.

For running the batch scripts directly on a host instead:

- Python 3.12 and packages listed in `requirements.txt`.
- Java 17 or newer for PySpark 4.x (Java 21 for the tested Windows environment).
- On Windows, Hadoop support files in `bin/` and a correctly configured
  `HADOOP_HOME`; the container does not need them.

```bash
python -m pip install -r requirements.txt
```

## Spark-based Analysis (Batch: Q1 and Q2)

Q1 and Q2 are answered by `src/batch/benchmark_layouts.py`, which runs the same
queries against the CSV, the single-file Parquet, and the month-partitioned
Parquet, three times each, and compares both the results and the timings.

The Docker command from Quick Start does this for you:

```bash
docker compose run --rm batch python src/batch/benchmark_layouts.py
```

For a host run, execute the three scripts in `src/batch/` in order
(`clean_requests.py`, `convert_to_parquet.py`, `benchmark_layouts.py`). See
`src/batch/cleaning_analysis_output.txt` for a recorded full run, including the
measured results and speedups.

## Kafka + Spark Structured Streaming (Q3)

The streaming pipeline replays historical NYC 311 complaints to Kafka and uses Spark Structured Streaming to count noise complaints for each ZIP code and one-hour event-time window.

### Input

`src/stream/prepare_replay.py` expects the Parquet input at:

```text
data/processing/311_Service_Requests_For_2026.parquet
```

This dataset file may be excluded from Git because of its size. Prepare it using the batch pipeline (Part 1 above) or place an equivalent dataset at this path before running the replay.

### Run on Windows (PowerShell)

Run the following commands from the repository root.

1. Start Kafka:

   ```powershell
   docker compose up -d
   ```

2. Configure Windows Hadoop and PySpark for the terminal session:

   ```powershell
   $env:HADOOP_HOME = (Get-Location).Path
   $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
   $env:PYSPARK_PYTHON = (Get-Command python).Source
   $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
   ```

3. Prepare a local SQLite database sorted by the complaint creation date:

   ```powershell
   python src/stream/prepare_replay.py
   ```

4. Create the Kafka topic with one partition, if it doesn't exist:

   ```powershell
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic nyc311-replay-full --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

   **Important:** Use an empty topic for each full replay. Re-running the producer on the same populated topic duplicates the complaint records.

5. Send complaints to Kafka:

   ```powershell
   python src/stream/producer.py
   ```

6. Check the Kafka ending offset:

   ```powershell
   docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic nyc311-replay-full
   ```

7. Run Spark's streaming consumer:

   ```powershell
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 src/stream/consumer.py
   ```

   The consumer uses `availableNow=True`, processes the current Kafka backlog in micro-batches, saves intermediate counts to local SQLite, and exports a final CSV when successful. Spark checkpoints are maintained at `src/stream/checkpoints/hourly_noise_full/`.

   If re-running with an existing checkpoint, keep its associated SQLite state. For a completely new replay, use new Kafka topic/output/checkpoint locations or carefully reset all of those together. Do not mix an old checkpoint with a new empty SQLite database.

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
