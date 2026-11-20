"""Shared paths and Windows Hadoop configuration for the pipeline.

The project paths and the Windows Hadoop bootstrap live here so each script
imports them instead of repeating the setup. Every Spark script calls
configure_hadoop() once, before it builds its SparkSession.
"""

import os
import sys
from pathlib import Path

# src/config/ sits two levels below the project root, so this works from any
# working directory.
ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSING = ROOT / "data" / "processing"
STREAM_DIR = Path(os.environ.get("STREAM_STATE_DIR", ROOT / "src" / "stream"))
OUTPUT_DIR = ROOT / "output"

# The raw CSV the batch pipeline reads: the NYC Open Data export of 2026
# service requests created up to 2026-10-19. It ships with the project in
# data/raw (data/ is not in Git because of its size).
RAW_CSV = DATA_RAW / "311_Service_Requests_For_2026_Until_20261019.csv"

# Writing local files with Spark on Windows needs winutils.exe + hadoop.dll.
# They live in bin/ at the project root, so HADOOP_HOME points there and
# Spark finds bin/winutils.exe.
BIN_DIR = ROOT / "bin"


def configure_hadoop():
    """Point Hadoop at the bundled winutils on Windows; no-op elsewhere."""
    if sys.platform != "win32":
        return

    for name in ("winutils.exe", "hadoop.dll"):
        if not (BIN_DIR / name).is_file():
            raise FileNotFoundError(f"Missing bundled Hadoop file: {BIN_DIR / name}")

    # Hadoop appends bin/ itself; HADOOP_HOME must be the project root.
    os.environ["HADOOP_HOME"] = str(ROOT)
    entries = os.environ.get("PATH", "").split(os.pathsep)
    os.environ["PATH"] = os.pathsep.join(
        [str(BIN_DIR)] + [entry for entry in entries if entry != str(BIN_DIR)]
    )
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
