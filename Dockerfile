# Batch pipeline image: Python 3.12 + Java + PySpark, matching requirements.txt.
# Spark 4.x runs on Java 17 or newer; the container uses the Debian 17 JRE.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    # The driver JVM defaults to 1 GB, which is not enough for the ~3.2M row
    # dataset; PYSPARK_SUBMIT_ARGS resizes it before the JVM starts.
    PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Only the requirement list is copied so the pip layer is cached when source
# changes. The project itself is bind-mounted by compose.yaml.
COPY requirements.txt ./
RUN pip install -r requirements.txt

CMD ["bash"]
