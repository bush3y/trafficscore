"""
City of Ottawa Development Applications ingestion.

Fetches planning applications from the City's ArcGIS REST API.
Filtered to high-signal types for large residential development:
  - Site Plan Control      (design approval for 10+ unit buildings)
  - Plan of Condominium    (creates individual unit titles — definitively multi-unit)
  - Official Plan Amendment (exceeds OP density limits — almost always large)
  - Zoning By-law Amendment (rezoning — indicates significant intensification)
  - Plan of Subdivision    (land division for larger development parcels)

After the main fetch, enriches matching records with data from OttWatch
(ottwatch.ca) — a community index of Ottawa dev applications that includes
project descriptions with storey counts and unit numbers.

Full refresh on each run (paginated at 1,000 records/page).

Usage:
    python -m ingestion.ottawa_development
"""

import os
import re
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

ENDPOINT = (
    "https://maps.ottawa.ca/arcgis/rest/services"
    "/Development_Applications/MapServer/0/query"
)

APP_TYPE_FILTER = (
    "APPLICATION_TYPE_EN IN ("
    "'Site Plan Control',"
    "'Plan of Condominium',"
    "'Official Plan Amendment',"
    "'Zoning By-law Amendment',"
    "'Plan of Subdivision'"
    ")"
)

# Definitively dead — exclude from results
EXCLUDE_STATUSES = {"Approval Lapsed"}

PAGE_SIZE = 1000

OTTWATCH_URL = "https://ottwatch.ca/devapp/map_data"


def fetch_page(offset):
    resp = requests.get(
        ENDPOINT,
        params={
            "where": APP_TYPE_FILTER,
            "outFields": (
                "OBJECTID,APPLICATION_NUMBER,APPLICATION_DATE,"
                "APPLICATION_TYPE_EN,OBJECT_CURRENT_STATUS_EN,"
                "OBJECT_CURRENT_STATUS_DATE,ADDRESS_NUMBER_ROAD_NAME,"
                "LATITUDE,LONGITUDE"
            ),
            "outSR": "4326",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("features", []), data.get("exceededTransferLimit", False)


def fetch_ottwatch():
    """Return dict mapping app_number -> {description, url} from OttWatch."""
    resp = requests.get(OTTWATCH_URL, timeout=30)
    resp.raise_for_status()
    features = resp.json().get("features", [])
    lookup = {}
    for f in features:
        props = f.get("properties") or {}
        app_number = (props.get("app_number") or "").strip()
        if not app_number:
            continue
        description = (props.get("description") or "").strip() or None
        lookup[app_number] = {"description": description}
    return lookup


def extract_storeys(text):
    """Extract storey count from a description string."""
    if not text:
        return None
    m = re.search(r'(\d+)[- ]?stor(?:e?y|eys)\b', text, re.IGNORECASE)
    if not m:
        m = re.search(r'(\d+)[- ]?sty\b', text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        return val if 1 <= val <= 200 else None
    return None


def extract_units(text):
    """Extract residential unit count from a description string."""
    if not text:
        return None
    m = re.search(r'(\d+)\s+(?:\w+\s+){0,2}units?\b', text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        return val if 1 <= val <= 10000 else None
    return None


def parse_epoch_ms(ms):
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).date()
    except (ValueError, OSError):
        return None


def run():
    pulled_at = datetime.now(timezone.utc)
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Self-applying migrations for OttWatch enrichment columns
    for col, col_type in [
        ("description", "text"),
        ("storeys", "smallint"),
        ("unit_count", "int"),
    ]:
        cur.execute(
            f"ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS {col} {col_type}"
        )

    cur.execute("TRUNCATE TABLE development_applications")

    total = 0
    skipped = 0
    offset = 0

    while True:
        print(f"  Fetching records {offset}–{offset + PAGE_SIZE - 1}...")
        features, has_more = fetch_page(offset)
        if not features:
            break

        for f in features:
            a = f.get("attributes", {})
            status = (a.get("OBJECT_CURRENT_STATUS_EN") or "").strip()

            if status in EXCLUDE_STATUSES:
                skipped += 1
                continue

            lat = a.get("LATITUDE")
            lon = a.get("LONGITUDE")
            if not lat or not lon:
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO development_applications
                    (objectid, application_number, application_date, application_type,
                     status, status_date, address, geometry, pulled_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            """, [
                a.get("OBJECTID"),
                (a.get("APPLICATION_NUMBER") or "").strip() or None,
                parse_epoch_ms(a.get("APPLICATION_DATE")),
                (a.get("APPLICATION_TYPE_EN") or "").strip() or None,
                status or None,
                parse_epoch_ms(a.get("OBJECT_CURRENT_STATUS_DATE")),
                (a.get("ADDRESS_NUMBER_ROAD_NAME") or "").strip() or None,
                float(lon),
                float(lat),
                pulled_at,
            ])
            total += 1

        offset += PAGE_SIZE
        if not has_more:
            break

    conn.commit()

    # OttWatch enrichment
    print("Fetching OttWatch data...")
    try:
        ottwatch = fetch_ottwatch()
        print(f"  {len(ottwatch)} OttWatch entries loaded.")
        rows = [
            (app_number, data["description"],
             extract_storeys(data["description"]),
             extract_units(data["description"]))
            for app_number, data in ottwatch.items()
        ]
        execute_values(cur, """
            UPDATE development_applications SET
                description  = v.description,
                storeys      = v.storeys::smallint,
                unit_count   = v.unit_count::int
            FROM (VALUES %s) AS v(app_number, description, storeys, unit_count)
            WHERE application_number = v.app_number
        """, rows)
        cur.execute("SELECT COUNT(*) FROM development_applications WHERE description IS NOT NULL")
        enriched = cur.fetchone()[0]
        conn.commit()
        print(f"  {enriched} records enriched with OttWatch data.")
    except Exception as e:
        conn.rollback()
        print(f"  OttWatch enrichment failed (non-fatal): {e}")

    cur.close()
    conn.close()
    print(f"Development applications: {total} saved, {skipped} skipped.")


if __name__ == "__main__":
    run()
