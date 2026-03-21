"""
TomTom Traffic Stats ingestion.

Submits an Area Analysis job for Ottawa, polls until complete,
downloads the GeoJSON result, and loads it into the tomtom_segments table.

Usage:
    python -m ingestion.tomtom_ingest
    python -m ingestion.tomtom_ingest --from 2024-01-01 --to 2024-12-31
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv

load_dotenv()

TOMTOM_API_KEY = os.environ["TOMTOM_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
BASE_URL = "https://api.tomtom.com/traffic/trafficstats"

# Ottawa bounding polygon (covers the full city)
# Note: geometry is nested under network.geometry per TomTom API spec
OTTAWA_POLYGON_COORDS = [[
    [-76.3554, 45.1189],
    [-75.2462, 45.1189],
    [-75.2462, 45.5376],
    [-76.3554, 45.5376],
    [-76.3554, 45.1189],
]]

# TomTom trial only exposes August 2024 data
# Paid accounts support up to 732 days of history
TRIAL_DATE_FROM = "2024-08-01"
TRIAL_DATE_TO = "2024-08-31"

# Time sets must be non-overlapping.
# For the POC we use a single all_day set.
TIME_SETS = [
    {
        "name": "all_day",
        "timeGroups": [{
            "days": ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"],
            "times": ["00:00-24:00"]
        }]
    },
]


def submit_job(date_from: str, date_to: str) -> str:
    url = f"{BASE_URL}/areaanalysis/1?key={TOMTOM_API_KEY}"
    payload = {
        "jobName": f"ottawa_{date_from}_{date_to}",
        "distanceUnit": "KILOMETERS",
        "network": {
            "name": "ottawa",
            "timeZoneId": "America/Toronto",
            "frcs": [0, 1, 2, 3, 4, 5, 6, 7],  # all road classes, 7 = residential
            "probeSource": "ALL",
            "geometry": {
                "type": "Polygon",
                "coordinates": OTTAWA_POLYGON_COORDS,
            },
        },
        "dateRange": {"name": date_from, "from": date_from, "to": date_to},
        "timeSets": TIME_SETS,
    }
    resp = requests.post(url, json=payload)
    if not resp.ok:
        print(f"API error: {resp.text}")
    resp.raise_for_status()
    job_id = resp.json()["jobId"]
    print(f"Job submitted: {job_id}")
    return job_id


def poll_until_done(job_id: str, poll_interval: int = 30, max_wait: int = 7200) -> dict:
    url = f"{BASE_URL}/status/1/{job_id}?key={TOMTOM_API_KEY}"
    start = time.time()
    while time.time() - start < max_wait:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] Job {job_id}: {status}")
        if status == "DONE":
            return data
        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Job failed with status: {status} — {data}")
        time.sleep(poll_interval)
    raise TimeoutError(f"Job {job_id} did not complete within {max_wait}s")


def download_geojson(result: dict) -> dict:
    """Find and download the GeoJSON result from the completed job."""
    # API returns a 'urls' list of download URLs
    urls = result.get("urls", [])
    for url in urls:
        if url and "geojson" in url.lower():
            print(f"Downloading GeoJSON...")
            resp = requests.get(url)
            resp.raise_for_status()
            return resp.json()

    print("Available URLs:", urls)
    raise ValueError("No GeoJSON URL found in job result")


def load_segments(geojson: dict, date_from: str, date_to: str, conn) -> int:
    """
    Parse GeoJSON features and upsert into tomtom_segments.

    TomTom response structure:
    - First feature has geometry=null and contains job metadata (skip it)
    - Each road segment feature has:
        properties.segmentId        — TomTom segment ID
        properties.frc              — Functional Road Class (7=residential)
        properties.streetName       — road name
        properties.speedLimit       — posted speed limit km/h
        properties.distance         — segment length in metres
        properties.segmentTimeResults — array of stats per time set:
            .sampleSize             — GPS probe count (our volume proxy)
            .averageSpeed           — km/h
            .speedPercentiles       — 19 values at 5th→95th percentile
                                      index 16 = 85th, index 18 = 95th
    """
    features = geojson.get("features", [])
    if not features:
        print("Warning: GeoJSON contains no features")
        return 0

    pulled_at = datetime.now()
    rows = []

    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry")

        # Skip the metadata header feature (no geometry)
        if not geom:
            continue

        tomtom_id = str(props.get("segmentId") or props.get("newSegmentId", ""))
        if not tomtom_id:
            continue

        frc = props.get("frc")
        road_name = props.get("streetName")

        # segmentTimeResults is a list — one entry per time set requested
        for result in props.get("segmentTimeResults", []):
            probe_count = result.get("sampleSize")
            avg_speed = result.get("averageSpeed")

            percentiles = result.get("speedPercentiles", [])
            p85 = percentiles[16] if len(percentiles) > 16 else None
            p95 = percentiles[18] if len(percentiles) > 18 else None

            rows.append((
                tomtom_id,
                frc,
                road_name,
                probe_count,
                avg_speed,
                p85,
                p95,
                "all_day",
                date_from,
                date_to,
                pulled_at,
                json.dumps(geom),
            ))

    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO tomtom_segments
            (tomtom_id, frc, road_name, probe_count, avg_speed_kmh,
             speed_p85_kmh, speed_p95_kmh, time_set, date_from, date_to,
             pulled_at, geometry)
        VALUES %s
        ON CONFLICT (tomtom_id, time_set, pulled_at) DO NOTHING
        """,
        rows,
        template=(
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
            " ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
        ),
    )
    conn.commit()
    cur.close()
    print(f"Loaded {len(rows)} segments into tomtom_segments")
    return len(rows)


def run(date_from: str = None, date_to: str = None):
    if not date_to:
        date_to = TRIAL_DATE_TO
    if not date_from:
        date_from = TRIAL_DATE_FROM

    print(f"Ottawa Traffic Stats pull: {date_from} → {date_to}")

    job_id = submit_job(date_from, date_to)
    print("Polling for completion (this typically takes 5-20 minutes)...")
    result = poll_until_done(job_id)

    print("Job complete. Downloading results...")
    geojson = download_geojson(result)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        count = load_segments(geojson, date_from, date_to, conn)
        print(f"Done. {count} road segments loaded.")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull Ottawa traffic stats from TomTom")
    parser.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD (default: 90 days ago)")
    parser.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    run(args.date_from, args.date_to)
