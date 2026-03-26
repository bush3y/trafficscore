"""
OC Transpo GTFS ingestion.

Auto-downloads the current GTFS feed from OC Transpo and loads bus route
geometries and weekday trip frequency into PostGIS. Safe to re-run —
truncates and reloads on each run to stay current with schedule changes.

OC Transpo updates their GTFS feed with each schedule change (~4x/year).

Usage:
    python -m ingestion.octranspo_gtfs
"""

import csv
import io
import os
import zipfile
from collections import defaultdict
from datetime import date

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

GTFS_URL = (
    "https://oct-gtfs-emasagcnfmcgeham.z01.azurefd.net/public-access/GTFSExport.zip"
)


def read_csv(zf, filename):
    """Read a CSV file from the GTFS zip, return list of dicts."""
    with zf.open(filename) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        return list(reader)


def stream_csv(zf, filename):
    """Stream a CSV file from the GTFS zip one row at a time (memory-efficient)."""
    with zf.open(filename) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        yield from reader


def run():
    print("Downloading OC Transpo GTFS feed...")
    resp = requests.get(GTFS_URL, timeout=60)
    resp.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    print(f"  Downloaded {len(resp.content) / 1024 / 1024:.1f} MB")

    # Routes: route_id → short name (e.g. "85", "270")
    print("Parsing routes...")
    routes = {r["route_id"]: r["route_short_name"] for r in read_csv(zf, "routes.txt")}
    print(f"  {len(routes)} routes")

    # Calendar: find service_ids active today that run on Monday.
    # GTFS files include multiple schedule periods — filter to the current one
    # to avoid inflating trip counts with future schedules.
    print("Parsing calendar...")
    today = date.today().strftime("%Y%m%d")
    weekday_services = set()
    for row in read_csv(zf, "calendar.txt"):
        if (row.get("monday") == "1"
                and row["start_date"] <= today <= row["end_date"]):
            weekday_services.add(row["service_id"])
    print(f"  {len(weekday_services)} active weekday service IDs (as of {today})")

    # Trips: count weekday trips per route, collect shape IDs
    print("Parsing trips...")
    route_trips = defaultdict(int)   # route_id → weekday trip count
    route_shapes = defaultdict(set)  # route_id → set of shape_ids
    for row in read_csv(zf, "trips.txt"):
        if row["service_id"] in weekday_services:
            route_trips[row["route_id"]] += 1
        if row.get("shape_id"):
            route_shapes[row["route_id"]].add(row["shape_id"])
    print(f"  {len(route_trips)} routes with weekday trips")

    # Shapes: stream row by row, keep only one shape's points in memory at a time.
    # GTFS shapes.txt is typically sorted by shape_id + sequence, so we can
    # process each shape and convert to WKT as soon as we see the next shape_id.
    needed_shape_ids = {sid for sids in route_shapes.values() for sid in sids}
    print(f"Parsing shapes ({len(needed_shape_ids)} needed, streaming)...")
    shape_wkt = {}
    current_id = None
    current_pts = []

    def flush_shape():
        if current_id and current_id in needed_shape_ids and current_pts:
            current_pts.sort(key=lambda x: x[0])
            coords = ", ".join(f"{lon} {lat}" for _, lon, lat in current_pts)
            shape_wkt[current_id] = f"LINESTRING({coords})"

    for row in stream_csv(zf, "shapes.txt"):
        sid = row["shape_id"]
        if sid != current_id:
            flush_shape()
            current_id = sid
            current_pts = []
        if sid in needed_shape_ids:
            current_pts.append((
                int(row["shape_pt_sequence"]),
                float(row["shape_pt_lon"]),
                float(row["shape_pt_lat"]),
            ))
    flush_shape()
    print(f"  {len(shape_wkt)} shapes loaded")

    # Load into DB
    print("Loading into database...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE bus_routes")

    inserted = skipped = 0
    for route_id, route_name in routes.items():
        shape_ids = route_shapes.get(route_id, set())
        linestrings = [shape_wkt[sid] for sid in shape_ids if sid in shape_wkt]

        if not linestrings:
            skipped += 1
            continue

        # Combine all shape variants into one MultiLineString
        inner = ", ".join(
            "(" + ls[len("LINESTRING("):-1] + ")" for ls in linestrings
        )
        multi_wkt = f"MULTILINESTRING({inner})"

        cur.execute("""
            INSERT INTO bus_routes (route_id, route_name, weekday_trips, geometry)
            VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
            ON CONFLICT (route_id) DO UPDATE SET
                route_name    = EXCLUDED.route_name,
                weekday_trips = EXCLUDED.weekday_trips,
                geometry      = EXCLUDED.geometry,
                fetched_at    = NOW()
        """, [route_id, route_name, route_trips.get(route_id, 0), multi_wkt])
        inserted += 1

    conn.commit()
    print(f"  {inserted} routes inserted, {skipped} skipped (no shapes)")
    cur.close()
    conn.close()


if __name__ == "__main__":
    run()
