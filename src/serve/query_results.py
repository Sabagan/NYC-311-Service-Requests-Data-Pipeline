"""Part 3 (Serve): answer the three questions from PostgreSQL and time them.

Each query runs several times. The first run can be slower (data not yet in
memory), so we report the first run and the median of all runs.

Run from the project root:
    python src/serve/query_results.py
    python src/serve/query_results.py --borough QUEENS --zip 11226 --date 2026-08-23
    python src/serve/query_results.py --explain     # also show each query plan
"""

import argparse
import statistics
import sys
import time
from pathlib import Path

# Make src/ importable when this file is run as a script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg

from serve.db import connect

# %(name)s placeholders are filled in by psycopg, which escapes the values,
# so user input can never change the SQL itself.
QUESTIONS = [
    {
        "label": "Q1",
        "question": "Which complaint types are most common in {borough}?",
        "sql": """
            SELECT complaint_type, request_count
            FROM q1_complaints_by_borough
            WHERE borough = %(borough)s
            ORDER BY request_count DESC, complaint_type
            LIMIT %(top)s""",
    },
    {
        "label": "Q2",
        "question": "How long does each agency take to close a request, "
                    "on average?",
        "sql": """
            SELECT agency,
                   avg_close_hours,
                   round(avg_close_hours / 24, 1) AS avg_close_days,
                   closed_requests
            FROM q2_agency_close_time
            ORDER BY avg_close_hours DESC""",
    },
    {
        "label": "Q3",
        "question": "How many noise complaints came in per hour in ZIP code "
                    "{zip} on {date}? (New York time)",
        # The stored windows are timestamps with time zone. The day boundaries
        # are midnight in New York, converted to the same type, so the range
        # can use the (incident_zip, window_start) primary key index.
        "sql": """
            SELECT to_char(window_start AT TIME ZONE 'America/New_York',
                           'YYYY-MM-DD HH24:MI') AS hour_start,
                   noise_count
            FROM q3_hourly_noise
            WHERE incident_zip = %(zip)s
              AND window_start >= (%(date)s::date::timestamp
                                   AT TIME ZONE 'America/New_York')
              AND window_start <  ((%(date)s::date + 1)::timestamp
                                   AT TIME ZONE 'America/New_York')
            ORDER BY window_start""",
    },
]


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--borough", default="BROOKLYN",
                        help="borough for Q1 (default: BROOKLYN)")
    parser.add_argument("--top", type=int, default=5,
                        help="how many complaint types Q1 lists (default: 5)")
    parser.add_argument("--zip", default="10456",
                        help="ZIP code for Q3 (default: 10456, the ZIP with "
                             "the most noise complaints)")
    parser.add_argument("--date", default="2026-07-26",
                        help="day for Q3, YYYY-MM-DD (default: 2026-07-26)")
    parser.add_argument("--runs", type=int, default=5,
                        help="times to run each query (default: 5)")
    parser.add_argument("--explain", action="store_true",
                        help="also print each query's plan (EXPLAIN ANALYZE)")
    args = parser.parse_args()
    args.borough = args.borough.upper()
    return args


def print_table(columns, rows):
    """Print rows as a simple aligned table."""
    cells = [[str(v) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells])
              for i, c in enumerate(columns)]
    line = "  ".join(c.ljust(w) for c, w in zip(columns, widths))
    print("  " + line)
    print("  " + "  ".join("-" * w for w in widths))
    for r in cells:
        print("  " + "  ".join(v.ljust(w) for v, w in zip(r, widths)))


def time_query(cur, sql, params, runs):
    """Run the query `runs` times; return its rows, columns and timings (ms)."""
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
        timings.append((time.perf_counter() - start) * 1000)
    columns = [d.name for d in cur.description]
    return rows, columns, timings


def main():
    args = parse_args()
    params = {"borough": args.borough, "top": args.top,
              "zip": args.zip, "date": args.date}
    summary = []

    with connect() as conn, conn.cursor() as cur:
        for q in QUESTIONS:
            question = q["question"].format(**params)
            rows, columns, timings = time_query(cur, q["sql"], params,
                                                args.runs)

            print(f"\n{q['label']}: {question}")
            if rows:
                print_table(columns, rows)
            else:
                print("  (no rows: check the value you asked about, or that "
                      "load_results.py has loaded this table)")
            if q["label"] == "Q3" and rows:
                print("  Hours with no noise complaints have no row.")

            median = statistics.median(timings)
            print(f"  Response time: first run {timings[0]:.2f} ms, "
                  f"median {median:.2f} ms over {args.runs} runs")
            summary.append((q["label"], len(rows), timings[0], median))

            if args.explain:
                # A client-side cursor puts the values into the SQL text,
                # which EXPLAIN needs.
                with psycopg.ClientCursor(conn) as explain_cur:
                    explain_cur.execute(
                        "EXPLAIN (ANALYZE, BUFFERS) " + q["sql"], params)
                    print("  Query plan:")
                    for (plan_line,) in explain_cur.fetchall():
                        print("    " + plan_line)

    print("\n--- Response times ---")
    print(f"{'question':<10}{'rows':>6}{'first run (ms)':>17}{'median (ms)':>14}")
    for label, n, first, median in summary:
        print(f"{label:<10}{n:>6}{first:>17.2f}{median:>14.2f}")


if __name__ == "__main__":
    main()
