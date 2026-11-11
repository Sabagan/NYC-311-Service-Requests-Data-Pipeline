from pathlib import Path
import pandas as pd

# Resolve the project root so these paths work from any working directory.
ROOT = Path(__file__).resolve().parents[2]
DATA_PROCESSING = ROOT / "data" / "processing"

# Input and output file paths
csv_file = (
    DATA_PROCESSING / "311_Service_Requests_For_2026_Until_20261019_clean.csv"
)
parquet_file = DATA_PROCESSING / "311_Service_Requests_For_2026.parquet"

# Read CSV file
df = pd.read_csv(csv_file, low_memory=False)

# Convert to Parquet with Snappy compression
# Can update row_group_size as needed
df.to_parquet(
    parquet_file,
    engine="pyarrow",
    compression="snappy",
    index=False
)

# Get file size in MB
csv_size_mb = csv_file.stat().st_size / 1_000_000
parquet_size_mb = parquet_file.stat().st_size / 1_000_000

# Calculate percentage reduction
reduction_pct = (
    (csv_size_mb - parquet_size_mb) / csv_size_mb * 100
)

# Report results
print(f"CSV file size:\t\t{csv_size_mb:.2f} MB")
print(f"Parquet file size:\t{parquet_size_mb:.2f} MB")
print(f"Storage reduction:\t{reduction_pct:.2f}%")

# Delete dataframe (just in case)
del df