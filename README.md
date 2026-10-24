# NYC 311 Service Requests Data Pipeline

## Dataset
https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9/about_data

## License
NYC Open Data (no restrictions on use; free public download), Terms of Use: https://data.cityofnewyork.us/stories/s/Terms-of-Use/k9k7-3cje/

## User-Raised Questions
1. Which complaint types are most common in a borough? (batch)
2. How long does each agency take to close a request, on average? (batch)
3. How many noise complaints came in per hour in each ZIP code tonight? (stream)

## Storage
- Download the CSV from Open Data NYC, containing all 2026 data until present data, then used the storage.py to convert to Parquet
- Used the API to get the data as Json in steps, then converted it Parquet

We can compare storage reduction efficiencies in terms of using Parquet rather than JSON and CSV.

## Spark-based Analysis
We will compare the performance of CSV vs Parquet in answering the first two (batch) questions
