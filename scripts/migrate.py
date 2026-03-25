"""
Run any pending DB migrations. Safe to re-run — all statements use
IF NOT EXISTS / IF EXISTS guards.

Usage:
    python -m scripts.migrate
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

MIGRATIONS = [
    ("neighbourhoods table", """
        CREATE TABLE IF NOT EXISTS neighbourhoods (
            id       SERIAL PRIMARY KEY,
            name     TEXT NOT NULL,
            geometry GEOMETRY(MultiPolygon, 4326) NOT NULL
        );
        CREATE INDEX IF NOT EXISTS neighbourhoods_geom_idx ON neighbourhoods USING GIST(geometry);
    """),
    ("bus_routes table", """
        CREATE TABLE IF NOT EXISTS bus_routes (
            route_id      TEXT PRIMARY KEY,
            route_name    TEXT NOT NULL,
            weekday_trips INTEGER DEFAULT 0,
            fetched_at    TIMESTAMPTZ DEFAULT NOW(),
            geometry      GEOMETRY(MultiLineString, 4326)
        );
        CREATE INDEX IF NOT EXISTS bus_routes_geom_idx ON bus_routes USING GIST(geometry);
    """),
]


def run():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()
    for name, sql in MIGRATIONS:
        print(f"  Applying: {name}...")
        cur.execute(sql)
    conn.commit()
    print(f"Done — {len(MIGRATIONS)} migrations applied.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    run()
