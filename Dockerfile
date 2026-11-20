# Shared image for batch and streaming pipelines.
# Python 3.12 + Java 17 + PySpark.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"

# Install Java runtime and process utilities.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies separately for Docker layer caching.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy application source and streaming entry point.
COPY src ./src
COPY docker/run_stream.py ./docker/run_stream.py
COPY docker/run_serve.py ./docker/run_serve.py

# Default command: streaming pipeline.
# Batch service overrides this command in compose.yaml.
CMD ["python", "-u", "docker/run_stream.py"]
