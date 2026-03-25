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

    # Calendar: find service_ids that run on a typical weekday (Monday)
    print("Parsing calendar...")
    weekday_services = set()
    for row in read_csv(zf, "calendar.txt"):
        if row.get("monday") == "1":
            weekday_services.add(row["service_id"])
    print(f"  {len(weekday_services)} weekday service IDs")

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

    # Shapes: build LineString WKT per shape_id
    print("Parsing shapes...")
    shape_points = defaultdict(list)  # shape_id → [(seq, lon, lat)]
    for row in read_csv(zf, "shapes.txt"):
        shape_points[row["shape_id"]].append((
            int(row["shape_pt_sequence"]),
            float(row["shape_pt_lon"]),
            float(row["shape_pt_lat"]),
        ))

    shape_wkt = {}
    for shape_id, pts in shape_points.items():
        pts.sort(key=lambda x: x[0])
        coords = ", ".join(f"{lon} {lat}" for _, lon, lat in pts)
        shape_wkt[shape_id] = f"LINESTRING({coords})"
    print(f"  {len(shape_wkt)} shapes")

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
