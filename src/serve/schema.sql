-- Part 3 (Serve): the tables that answer our three questions.
--
-- Each table's PRIMARY KEY is chosen to match how its question asks, so
-- PostgreSQL can jump to the matching rows through the key's index instead
-- of reading the whole table (Week 2: CSV has "no index, no way to skip").

-- Q1: Which complaint types are most common in a borough?
-- Asked by borough, so the key starts with borough.
CREATE TABLE IF NOT EXISTS q1_complaints_by_borough (
    borough        TEXT   NOT NULL,
    complaint_type TEXT   NOT NULL,
    request_count  BIGINT NOT NULL CHECK (request_count > 0),
    PRIMARY KEY (borough, complaint_type)
);

-- Q2: How long does each agency take to close a request, on average?
-- One row per agency.
CREATE TABLE IF NOT EXISTS q2_agency_close_time (
    agency          TEXT           PRIMARY KEY,
    avg_close_hours NUMERIC(12, 2) NOT NULL CHECK (avg_close_hours >= 0),
    closed_requests BIGINT         NOT NULL CHECK (closed_requests > 0)
);

-- Q3: How many noise complaints come in per hour in each ZIP code?
-- Asked by ZIP code and then a time range, so the key is (ZIP, hour).
CREATE TABLE IF NOT EXISTS q3_hourly_noise (
    incident_zip TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    noise_count  BIGINT      NOT NULL CHECK (noise_count > 0),
    PRIMARY KEY (incident_zip, window_start)
);

-- Part 4: rows that failed a quality check are set aside here instead of
-- being dropped, with the check they failed and the original row.
CREATE TABLE IF NOT EXISTS rejected_rows (
    id          BIGSERIAL   PRIMARY KEY,
    dataset     TEXT        NOT NULL,
    reason      TEXT        NOT NULL,
    raw_row     JSONB       NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
