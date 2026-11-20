"""How the Part 3 scripts connect to PostgreSQL.

The defaults match the postgres service in compose.yaml, reached from your
laptop on localhost:5432. Inside Docker, compose.yaml sets PGHOST=postgres
so the same code finds the database container instead.
"""

import os

import psycopg


def connect():
    """Open a connection to the serving database."""
    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "nyc311"),
        password=os.environ.get("PGPASSWORD", "nyc311"),
        dbname=os.environ.get("PGDATABASE", "nyc311"),
    )
