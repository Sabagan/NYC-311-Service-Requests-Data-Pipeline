# NYC 311 Service Requests Data Pipeline

A two-part data pipeline over NYC 311 service requests, built with PySpark and
Kafka and run entirely through Docker.

| Part | What it does | Stack |
| --- | --- | --- |
| **Part 1 – Store and batch** | Cleans the raw CSV, converts it to Parquet (single file and month-partitioned), then benchmarks the three storage layouts against each other. | PySpark batch, Docker |
| **Part 2 – Streaming** | Replays the cleaned records through Kafka and counts noise complaints per ZIP code and one-hour event-time window. | Kafka, Spark Structured Streaming |

## Questions Answered

| # | Question | Part |
| --- | --- | --- |
| Q1 | Which complaint types are most common in each borough? | 1 (batch) |
| Q2 | How long does each agency take to close a request, on average? | 1 (batch) |
| Q3 | How many noise complaints arrived per hour in each ZIP code? | 2 (streaming) |

## Dataset

[NYC 311 Service Requests from 2020 to Present](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data)

The pipeline uses the records created between 2026-01-01 and 2026-10-19 from
that dataset. The portal's CSV export for that range is kept at:

```text
data/raw/311_Service_Requests_For_2026_Until_20261019.csv
```

It contains 3,174,685 rows (~2.8 GB) with the portal's human-readable column
names, which is what the batch scripts expect. The file ships with this copy of
the project, but `data/` is excluded from Git because of its size: if it is
missing, download the same date range from the link above and save it under
exactly that filename, or the pipeline stops with `Input file not found`.

### License

NYC Open Data: free public download with no restrictions on use.
[Terms of Use](https://data.cityofnewyork.us/stories/s/Terms-of-Use/k9k7-3cje/)

## Setup and Prerequisites

Everything below runs through Docker, so the only requirement is:

- Docker Desktop with Docker Compose, with **at least 8 GB of memory**
  allocated (the Spark driver is configured for 4 GB in the image).
- Internet access on the first run, to pull the image, dependencies, and the
  Spark Kafka connector.

Both pipelines share one image: Python 3.12, Java 17, and PySpark 4.3.0
(`Dockerfile`, `requirements.txt`).

<details>
<summary>Running the scripts directly on the host instead</summary>

- Python 3.12 with `python -m pip install -r requirements.txt`.
- Java 17 or newer for PySpark 4.x (Java 21 was used for the tested Windows
  environment).
- On Windows only: the Hadoop support files in `bin/` (`winutils.exe`,
  `hadoop.dll`). `src/config/spark_config.py` points `HADOOP_HOME` at them, so
  no manual environment setup is needed. The container uses Linux Hadoop and
  does not need them.
- On Windows, run from the repository root so the relative paths resolve.

</details>

## Part 1: Store and Batch

Part 1 runs in one container and executes three PySpark scripts in order:

1. **`src/batch/clean_requests.py`** – validates rows (empty rows, missing or
   duplicate IDs, unparseable or out-of-year dates, invalid boroughs, missing
   agencies), normalizes text and timestamps, writes the clean CSV, then reads
   it back and verifies it.
2. **`src/batch/convert_to_parquet.py`** – writes both Parquet layouts from the
   clean CSV: one single file, and a month-partitioned directory laid out as
   `created_month=YYYY-MM/part-*.parquet`.
3. **`src/batch/benchmark_layouts.py`** – runs Q1 and Q2 against all three
   layouts (CSV, Parquet, month-partitioned Parquet) three times each, then
   prints the timings, a results-equality check, and the speedups.

### Run it

With Docker Desktop running, from the repository root:

```bash
docker compose --profile batch run --build --rm batch
```

That builds the image if needed, runs all three steps, and removes the
container when it finishes. To do it in two commands instead:

```bash
docker compose build batch
docker compose run --rm batch
```

Every run reprocesses the raw CSV from scratch, so allow **15–60 minutes**
depending on the machine. The repository is mounted into the container, so all
output lands on the host in `data/` exactly as it would for a host run.

Run a single step, for example only the benchmark:

```bash
docker compose run --rm batch python src/batch/benchmark_layouts.py
```

### Outputs

| Path | Contents |
| --- | --- |
| `data/processing/311_Service_Requests_For_2026_Until_20261019_clean.csv` | Cleaned CSV, original schema and column order, 3,171,200 rows (99.89% of the input). |
| `data/processing/311_Service_Requests_For_2026.parquet` | Single-file Parquet, Snappy-compressed. |
| `data/processing/311_Service_Requests_For_2026_month_partitioned.parquet/` | Partitioned Parquet, one file per month. Spark discovers `created_month` from the folder names, so a query that filters on it reads only the matching months (partition pruning). |

The only rejected rows were 3,485 with an invalid borough; the clean CSV passed
all verification checks.

### Recorded Results

From `src/batch/cleaning_analysis_output.txt` (a full recorded run):

| Layout | Size | Storage reduction vs CSV |
| --- | ---: | ---: |
| CSV | 2,595.37 MB | — |
| Parquet (single file) | 371.89 MB | 85.67% |
| Month-partitioned Parquet (10 partitions) | 365.37 MB | 85.92% (−1.75% vs single file) |

Median query time over three runs (speedup against the CSV in parentheses):

| Layout | Q1: complaint types by borough | Q2: average closure time by agency |
| --- | ---: | ---: |
| CSV | 10.76 s | 9.62 s |
| Parquet | 2.58 s (4.16x) | 6.73 s (1.43x) |
| Month-partitioned Parquet | 3.63 s (2.96x) | 5.00 s (1.92x) |

Both Parquet layouts returned results identical to the CSV run. Partitioning
pays off where the query can prune months (Q2, 1.34x faster than the single
file); on Q1, which scans every row, the extra directory listing costs a little
(0.71x). See `src/batch/cleaning_analysis_output.txt` for the full output and
all per-borough rankings.

## Part 2: Streaming with Kafka

The streaming pipeline replays the historical records to Kafka and aggregates
them with Spark Structured Streaming, answering Q3.

### Prerequisite

Part 2 reads the single-file Parquet produced by Part 1:

```text
data/processing/311_Service_Requests_For_2026.parquet
```

Run Part 1 first, or place an equivalent dataset at that path. Compose mounts
`data/processing` read-only, so the file must exist on the host before you
start.

### Run it

With Docker Desktop running and the Parquet file in place, from the repository
root:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from streaming
```

Compose starts Kafka and waits for it to report healthy, then the streaming
container:

1. **`src/stream/prepare_replay.py`** – sorts the Parquet input into SQLite
   (`events_sorted.sqlite`) so records can be produced in chronological order.
2. **`src/stream/producer.py`** – produces every record to the Kafka topic,
   keyed by ZIP code.
3. **`src/stream/consumer.py`** – reads the topic with Structured Streaming,
   keeps noise complaints with a non-empty ZIP, applies a 2-hour watermark,
   counts them per ZIP and one-hour event-time window, stores the counts in
   SQLite, then exports `output/hourly_noise_full.csv`.

`docker/run_stream.py` orchestrates the three scripts, creating a new Kafka
topic and an isolated SQLite/checkpoint directory in the `stream-runs` volume
for each invocation. That means every run is a fresh full replay, not a
resume; the final CSV is replaced only on success. Do not run two copies
concurrently, since they share the output path.

Teardown:

```bash
docker compose down      # remove containers, keep run data
docker compose down -v   # also discard Kafka data and replay state
```

Neither command deletes your mounted Parquet input or the output CSV.

### Output

`output/hourly_noise_full.csv`:

| Column | Meaning |
| --- | --- |
| `window_start` | Start of the one-hour event-time window (UTC in the exported CSV) |
| `window_end` | End of the window, exclusive (UTC in the exported CSV) |
| `incident_zip` | ZIP code associated with the complaints |
| `noise_count` | Number of eligible noise complaint records in that ZIP-hour window |

Each row describes one ZIP code during one hour. A ZIP-hour combination with no
complaints has no row; it is never written as a zero.

### Verified Full-Run Results

| Metric | Observed result |
| --- | ---: |
| Total NYC 311 records replayed | 3,171,200 |
| Kafka topic ending offset (partition 0) | 3,171,200 |
| Expected noise complaints with nonempty ZIP codes | 615,086 |
| Total `noise_count` from Spark | 615,086 |
| Unique ZIP-hour groups | 321,010 |
| Count difference | 0 |

The expected count came from querying the sorted source records in
`events_sorted.sqlite`; the actual count is the sum of `noise_count` across all
ZIP-hour groups. The totals match, which confirms no eligible complaints were
lost, though matching totals alone do not prove every individual ZIP-hour count
is correct. See `src/stream/streaming_analysis_output.txt` for the summary.

## Repository Layout

- `src/batch/` – the three Part 1 PySpark scripts and their recorded run output.
- `src/stream/` – `prepare_replay.py`, `producer.py`, `consumer.py` and the
  recorded streaming output. SQLite state and Spark checkpoints are generated
  here on host runs.
- `src/config/spark_config.py` – shared project paths and Windows Hadoop setup.
- `docker/run_stream.py` – streaming entry point that runs the three stream
  scripts in order under a fresh run ID.
- `bin/` – `winutils.exe` and `hadoop.dll`, needed only for host runs on
  Windows.
- `data/raw/` – the original CSV and temporary build files.
- `data/processing/` – cleaned CSV and both Parquet layouts.
- `output/` – final streaming output, `hourly_noise_full.csv`.
- `Dockerfile`, `compose.yaml`, `requirements.txt` – the shared image and the
  `kafka`, `batch`, and `streaming` services.
