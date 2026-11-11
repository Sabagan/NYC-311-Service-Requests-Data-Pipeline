"""Shared configuration for the batch Spark scripts.

The project paths and the Windows Hadoop bootstrap live here so each script
imports them instead of repeating the setup. Every script calls
configure_hadoop() once, before it builds its SparkSession.
"""

import os
import sys
from pathlib import Path

# batch/ sits one level below the project root, so this works from any working
# directory.
ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSING = ROOT / "data" / "processing"

# Writing local files with Spark on Windows needs winutils.exe + hadoop.dll.
# They live in the project root under winutils/bin.
WINUTILS_DIR = ROOT / "winutils"


def configure_hadoop():
    """Point Hadoop at the bundled winutils on Windows; no-op elsewhere."""
    if sys.platform != "win32":
        return

    os.environ.setdefault("HADOOP_HOME", str(WINUTILS_DIR))
    os.environ["PATH"] = (
        str(WINUTILS_DIR / "bin") + os.pathsep + os.environ["PATH"]
    )
