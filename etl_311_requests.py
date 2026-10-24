
import time
import requests
import pandas as pd
import os

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
START = "2026-01-01T00:00:00"
END = "2027-01-01T00:00:00"
PAGE_SIZE = 50_000

session = requests.Session()
session.headers.update({
    "User-Agent": "nyc-311-data-pipeline/1.0"
})


# ============================================================
# 1. EXTRACT
# ============================================================
# Retrieve all 311 requests created during 2026 from the API.
# Includes pagination, expected-count reconciliation, and
# source-data integrity checks.

def fetch_all_2026():
    where = (
        f"created_date >= '{START}' "
        f"AND created_date < '{END}'"
    )

    # Get the expected number of matching records.
    response = session.get(
        BASE_URL,
        params={
            "$select": "count(*) AS total",
            "$where": where,
        },
        timeout=120,
    )
    response.raise_for_status()
    expected = int(response.json()[0]["total"])

    records = []
    offset = 0

    # Retrieve the results page by page.
    while offset < expected:
        response = session.get(
            BASE_URL,
            params={
                "$where": where,
                "$order": "unique_key ASC",
                "$limit": PAGE_SIZE,
                "$offset": offset,
            },
            timeout=180,
        )
        response.raise_for_status()
        page = response.json()

        if not page:
            raise RuntimeError(
                f"Unexpected empty page at offset {offset}; "
                f"expected {expected} records."
            )

        records.extend(page)
        offset += len(page)

        print(f"Fetched {len(records):,} / {expected:,}")

        if len(page) < PAGE_SIZE and len(records) < expected:
            raise RuntimeError(
                "Pagination ended before expected count."
            )

        time.sleep(0.1)

    # Convert the API's JSON records into a DataFrame.
    df = pd.DataFrame(records)

    # Extraction-level integrity checks.
    if len(df) != expected:
        raise RuntimeError(
            f"Count mismatch: expected {expected}, got {len(df)}"
        )

    if df["unique_key"].duplicated().any():
        raise RuntimeError(
            "Duplicate unique_key values in extraction."
        )

    return df


raw = fetch_all_2026()

# Extraction validation: verify IDs and date boundaries.
assert len(raw) > 0, "No records extracted"
assert raw["unique_key"].notna().all(), "Null source IDs"
assert not raw["unique_key"].duplicated().any(), (
    "Duplicate source IDs"
)

created = pd.to_datetime(
    raw["created_date"], errors="coerce"
)
assert created.notna().all(), "Invalid creation timestamps"
assert created.ge("2026-01-01").all()
assert created.lt("2027-01-01").all()


# ============================================================
# 2. TRANSFORM
# ============================================================
# Standardize values, validate business rules, and identify
# records that need to be quarantined.

VALID_BOROUGHS = {
    "BRONX",
    "BROOKLYN",
    "MANHATTAN",
    "QUEENS",
    "STATEN ISLAND",
}


# Reference-data lookup.
# NOTE: Replace observed agency codes with an authoritative,
# reviewed, versioned list for production use.
def get_observed_agencies():
    response = session.get(
        BASE_URL,
        params={
            "$select": "DISTINCT agency",
            "$where": "agency IS NOT NULL",
            "$limit": 5000,
            "$order": "agency ASC",
        },
        timeout=120,
    )
    response.raise_for_status()

    return {
        r["agency"].strip().upper()
        for r in response.json()
        if r.get("agency") and r["agency"].strip()
    }


VALID_AGENCIES = get_observed_agencies()

# Work on a copy so the raw extraction remains unchanged.
df = raw.copy()

# 2.1 Normalize text fields.
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].map(
        lambda x: x.strip() if isinstance(x, str) else x
    )
    df[col] = df[col].replace("", pd.NA)

# 2.2 Parse timestamps.
df["created_date"] = pd.to_datetime(
    df["created_date"], errors="coerce"
)
df["closed_date"] = pd.to_datetime(
    df["closed_date"], errors="coerce"
)

# 2.3 Standardize categorical fields.
df["borough"] = df["borough"].str.upper()
df["agency"] = df["agency"].str.upper()

# 2.4 Run data-quality checks.
# True means the record violates the corresponding rule.

df["dq_missing_id"] = (
    df["unique_key"].isna()
    | df["unique_key"].astype("string").str.strip().eq("")
)

df["dq_duplicate_id"] = (
    df["unique_key"].notna()
    & df["unique_key"].duplicated(keep=False)
)

df["dq_invalid_created_date"] = (
    df["created_date"].isna()
    | (df["created_date"] < pd.Timestamp("2026-01-01"))
    | (df["created_date"] >= pd.Timestamp("2027-01-01"))
)

df["dq_invalid_borough"] = ~df["borough"].isin(
    VALID_BOROUGHS
)

df["dq_invalid_agency"] = ~df["agency"].isin(
    VALID_AGENCIES
)

df["dq_closed_before_created"] = (
    df["closed_date"].notna()
    & df["created_date"].notna()
    & (df["closed_date"] < df["created_date"])
)

# 2.5 Classify records.
critical_flags = [
    "dq_missing_id",
    "dq_duplicate_id",
    "dq_invalid_created_date",
    "dq_invalid_borough",
    "dq_invalid_agency",
]

df["dq_pass"] = ~df[critical_flags].any(axis=1)

clean = df.loc[df["dq_pass"]].copy()
quarantine = df.loc[~df["dq_pass"]].copy()

# 2.6 Transformation reconciliation.
assert len(clean) + len(quarantine) == len(df), (
    "Records were lost during transformation."
)

# 2.7 Generate the quality report.
flag_cols = [
    c for c in df.columns
    if c.startswith("dq_") and c != "dq_pass"
]

print("Total extracted:", len(df))
print("Clean records:", len(clean))
print("Quarantined records:", len(quarantine))
print("\nFailures by rule:")
print(df[flag_cols].sum().sort_values(ascending=False))

print("\nBorough distribution:")
print(df["borough"].value_counts(dropna=False))

print("\nAgency distribution:")
print(df["agency"].value_counts(dropna=False))

print("\nMissing-value percentages:")
print(
    (df.isna().mean() * 100)
    .round(2)
    .sort_values(ascending=False)
    .head(20)
)


# ============================================================
# 3. LOAD
# ============================================================
# Persist the raw extraction, clean dataset, and quarantined
# records as Parquet files for downstream analytics.

raw.to_json(
    "nyc_311_2026_raw.json",
    orient="records",
    date_format="iso"
)
raw.to_parquet(
    "nyc_311_2026_raw.parquet",
    index=False
)

clean.to_json(
    "nyc_311_2026_clean.json",
    orient="records",
    date_format="iso"
)
clean.to_parquet(
    "nyc_311_2026_clean.parquet",
    index=False
)

quarantine.to_json(
    "nyc_311_2026_quarantine.json",
    orient="records",
    date_format="iso"
)
quarantine.to_parquet(
    "nyc_311_2026_quarantine.parquet",
    index=False
)


# ============================================================
# 4. COMPARE STORAGE SIZES
# ============================================================

for type in ["raw", "clean", "quarantine"]:
    json_size_bytes = os.path.getsize(f"nyc_311_2026_${type}.json")
    parquet_size_bytes = os.path.getsize(f"nyc_311_2026_${type}.parquet")

    # Convert bytes to megabytes (MB).
    json_size_mb = json_size_bytes / (1024 ** 2)
    parquet_size_mb = parquet_size_bytes / (1024 ** 2)

    # Calculate percentage reduction relative to JSON.
    reduction_pct = (
        (json_size_bytes - parquet_size_bytes)
        / json_size_bytes
    ) * 100 if json_size_bytes else 0

    print("\n--- Storage Comparison ---")
    print(f"JSON size:       {json_size_mb:,.2f} MB")
    print(f"Parquet size:    {parquet_size_mb:,.2f} MB")
    print(f"Space saved:     {json_size_mb - parquet_size_mb:,.2f} MB")
    print(f"Storage reduced: {reduction_pct:.2f}%")