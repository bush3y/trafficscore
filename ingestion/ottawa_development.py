"""
City of Ottawa Development Applications ingestion.

Fetches planning applications from the City's ArcGIS REST API.
Filtered to high-signal types for large residential development:
  - Site Plan Control      (design approval for 10+ unit buildings)
  - Plan of Condominium    (creates individual unit titles — definitively multi-unit)
  - Official Plan Amendment (exceeds OP density limits — almost always large)
  - Zoning By-law Amendment (rezoning — indicates significant intensification)
  - Plan of Subdivision    (land division for larger development parcels)

Full refresh on each run (paginated at 1,000 records/page).

Usage:
    python -m ingestion.ottawa_development
"""

import os
from datetime import datetime, timezone

import psycopg2
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
    cur.close()
    conn.close()
    print(f"Development applications: {total} saved, {skipped} skipped.")


if __name__ == "__main__":
    run()
