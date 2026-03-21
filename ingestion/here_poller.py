"""
HERE Traffic Flow poller.

Queries the HERE Traffic Flow API v7 for Ottawa using a bounding box
and stores speed/congestion observations. Run on a schedule (every 2 hours).

Ottawa is split into 4 quadrants to keep each bounding box manageable.

Usage:
    python -m ingestion.here_poller          # single poll
    python -m ingestion.here_poller --schedule  # run on 2-hour schedule
"""

import argparse
import json
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv

load_dotenv()

HERE_API_KEY = os.environ["HERE_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
HERE_FLOW_URL = "https://data.traffic.hereapi.com/v7/flow"

# Ottawa split into 4 quadrants (west/east, north/south)
# Format: (label, min_lon, min_lat, max_lon, max_lat)
OTTAWA_QUADRANTS = [
    ("NW", -76.3554, 45.3500, -75.8000, 45.5376),
    ("NE", -75.8000, 45.3500, -75.2462, 45.5376),
    ("SW", -76.3554, 45.1189, -75.8000, 45.3500),
    ("SE", -75.8000, 45.1189, -75.2462, 45.3500),
]


def fetch_quadrant(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> dict:
    params = {
        "in": f"bbox:{min_lon},{min_lat},{max_lon},{max_lat}",
        "locationReferencing": "shape",
        "apiKey": HERE_API_KEY,
    }
    resp = requests.get(HERE_FLOW_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_flow_items(data: dict) -> list[tuple]:
    """Extract flow observations from HERE API response."""
    rows = []
    observed_at = datetime.now()

    for item in data.get("results", []):
        current_flow = item.get("currentFlow", {})
        location = item.get("location", {})

        # HERE link ID from location reference
        here_link_id = None
        for ref in location.get("locationReference", []):
            here_link_id = ref.get("id") or ref.get("hereMapVersion")
            if here_link_id:
                break

        if not here_link_id:
            here_link_id = str(hash(str(location)))  # fallback

        # Build geometry from shape points if available
        geom = None
        shape = item.get("location", {}).get("shape", {})
        if shape:
            geom = json.dumps(shape)

        rows.append((
            here_link_id,
            observed_at,
            current_flow.get("speed"),        # m/s
            current_flow.get("freeFlow"),     # m/s
            current_flow.get("jamFactor"),    # 0-10
            current_flow.get("confidence"),   # 0-1
            current_flow.get("traversability"),
            geom,
        ))

    return rows


def load_observations(rows: list[tuple], conn):
    if not rows:
        return

    cur = conn.cursor()

    # Rows without geometry
    no_geom = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows if not r[7]]
    # Rows with geometry
    with_geom = [r for r in rows if r[7]]

    if no_geom:
        execute_values(
            cur,
            """
            INSERT INTO here_flow_observations
                (here_link_id, observed_at, speed_ms, free_flow_ms,
                 jam_factor, confidence, traversability)
            VALUES %s
            """,
            no_geom,
        )

    if with_geom:
        execute_values(
            cur,
            """
            INSERT INTO here_flow_observations
                (here_link_id, observed_at, speed_ms, free_flow_ms,
                 jam_factor, confidence, traversability, geometry)
            VALUES %s
            """,
            with_geom,
            template=(
                "(%s, %s, %s, %s, %s, %s, %s,"
                " ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
            ),
        )

    conn.commit()
    cur.close()


def poll_once():
    conn = psycopg2.connect(DATABASE_URL)
    total = 0
    try:
        for label, min_lon, min_lat, max_lon, max_lat in OTTAWA_QUADRANTS:
            print(f"  Polling quadrant {label}...")
            data = fetch_quadrant(min_lon, min_lat, max_lon, max_lat)
            rows = parse_flow_items(data)
            load_observations(rows, conn)
            total += len(rows)
            print(f"    {len(rows)} observations stored")
    finally:
        conn.close()
    print(f"Poll complete — {total} total observations at {datetime.now().strftime('%Y-%m-%d %H:%M')}")


def run_scheduled():
    from apscheduler.schedulers.blocking import BlockingScheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(poll_once, "interval", hours=2, next_run_time=datetime.now())
    print("HERE poller scheduled every 2 hours. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("Scheduler stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poll HERE Traffic Flow for Ottawa")
    parser.add_argument("--schedule", action="store_true", help="Run on 2-hour schedule")
    args = parser.parse_args()

    if args.schedule:
        run_scheduled()
    else:
        poll_once()
