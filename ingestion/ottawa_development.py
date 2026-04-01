"""
City of Ottawa Development Applications ingestion.

Phase 1 — ArcGIS fetch:
  Fetches planning applications from the City's ArcGIS REST API.
  Filtered types (broaden by editing APP_TYPE_FILTER):
    - Site Plan Control      (design approval for significant buildings)
    - Plan of Condominium    (creates individual unit titles)
    - Official Plan Amendment (exceeds OP density limits)
    - Plan of Subdivision    (land division)
    - Demolition Control     (what's coming down — signals redevelopment)

  Uses UPSERT on objectid so enrichment columns survive monthly re-runs.
  Stale records (not seen in this run) are deleted after upsert.

Phase 2 — devapps enrichment (incremental):
  For each application_number not yet enriched, hits:
    GET https://devapps-restapi.ottawa.ca/devapps/{appNumber}?authKey=...
  Stores description, parsed fields, planner info, and documents.
  Records that return 404 are marked as fetched so they aren't retried.

Usage:
    python -m ingestion.ottawa_development
"""

import os
import re
import time
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

ARCGIS_ENDPOINT = (
    "https://maps.ottawa.ca/arcgis/rest/services"
    "/Development_Applications/MapServer/0/query"
)

DEVAPPS_BASE = "https://devapps-restapi.ottawa.ca/devapps"
DEVAPPS_AUTH_KEY = "4r5T2egSmKm5"
DEVAPPS_HEADERS = {
    "Origin": "https://devapps.ottawa.ca",
    "Referer": "https://devapps.ottawa.ca/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# Broaden this filter to capture more types in the future
APP_TYPE_FILTER = (
    "APPLICATION_TYPE_EN IN ("
    "'Site Plan Control',"
    "'Plan of Condominium',"
    "'Official Plan Amendment',"
    "'Plan of Subdivision',"
    "'Demolition Control'"
    ")"
)

# Definitively dead — exclude from results
EXCLUDE_STATUSES = {"Approval Lapsed"}

PAGE_SIZE = 1000

# How long to wait between devapps API calls (be polite)
DEVAPPS_SLEEP = 0.15


# ---------------------------------------------------------------------------
# ArcGIS fetch
# ---------------------------------------------------------------------------

def fetch_arcgis_page(offset):
    resp = requests.get(
        ARCGIS_ENDPOINT,
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


# ---------------------------------------------------------------------------
# devapps API fetch
# ---------------------------------------------------------------------------

def fetch_devapp(app_number):
    """Fetch full application detail from devapps API. Returns dict or None."""
    url = f"{DEVAPPS_BASE}/{app_number}?authKey={DEVAPPS_AUTH_KEY}"
    try:
        resp = requests.get(url, headers=DEVAPPS_HEADERS, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException:
        return None


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def extract_storeys(text):
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
    if not text:
        return None
    # "18 dwelling units", "18 units", "18-unit building"
    m = re.search(r'(\d+)[- ](?:\w+[- ]){0,2}(?:dwelling[- ])?units?\b', text, re.IGNORECASE)
    if not m:
        m = re.search(r'(\d+)\s+(?:\w+\s+){0,2}(?:dwelling\s+)?units?\b', text, re.IGNORECASE)
    if m:
        val = int(m.group(1))
        return val if 1 <= val <= 10000 else None
    return None


def extract_use_type(text):
    if not text:
        return None
    t = text.lower()
    residential = any(x in t for x in [
        'residential', 'dwelling', 'apartment', 'townhouse', 'housing', 'units',
        'retirement', 'long-term care', 'affordable housing',
    ])
    commercial = any(x in t for x in [
        'commercial', 'retail', 'office', 'hotel', 'restaurant', 'mixed-use', 'mixed use',
    ])
    if residential and commercial:
        return 'mixed'
    if residential:
        return 'residential'
    if commercial:
        return 'commercial'
    if any(x in t for x in ['industrial', 'warehouse', 'manufacturing']):
        return 'industrial'
    return None


def extract_building_type(text):
    if not text:
        return None
    t = text.lower()
    if 'stacked townhouse' in t or 'stacked town' in t:
        return 'stacked_townhouse'
    if 'townhouse' in t or 'town house' in t:
        return 'townhouse'
    if 'retirement' in t or 'long-term care' in t or 'long term care' in t:
        return 'retirement_home'
    if 'apartment' in t:
        return 'apartment'
    if 'condominium' in t or 'condo' in t:
        return 'condominium'
    if 'mixed-use' in t or 'mixed use' in t:
        return 'mixed_use'
    if 'office' in t:
        return 'office'
    if 'retail' in t:
        return 'retail'
    if 'hotel' in t or 'inn' in t:
        return 'hotel'
    if ('single' in t and ('detach' in t or 'family' in t)):
        return 'single_family'
    return None


def extract_parking(text):
    if not text:
        return None
    t = text.lower()
    if 'no vehicular parking' in t or 'no surface parking' in t:
        return 0
    m = re.search(r'(\d[\d,]*)\s+(?:surface\s+|underground\s+|at-grade\s+)?parking\s+space', t)
    if m:
        val = int(m.group(1).replace(',', ''))
        return val if val < 5000 else None
    m = re.search(r'(\d[\d,]*)\s+parking\b', t)
    if m:
        val = int(m.group(1).replace(',', ''))
        return val if val < 5000 else None
    return None


def extract_gfa(text):
    if not text:
        return None
    m = re.search(r'([\d,]+)\s*(?:m²|m2|sq\.?\s*m\.?|square\s+metres?)', text, re.IGNORECASE)
    if m:
        val = int(m.group(1).replace(',', ''))
        return val if 10 <= val <= 2_000_000 else None
    m = re.search(r'([\d,]+)[- ]square[- ]metre', text, re.IGNORECASE)
    if m:
        val = int(m.group(1).replace(',', ''))
        return val if 10 <= val <= 2_000_000 else None
    return None


def infer_doc_type(name):
    t = name.lower()
    if any(x in t for x in ['transportation impact', 'traffic impact', 'traffic study',
                              'transportation study', 'transportation brief', 'transportation report',
                              'tis ', 'tia ']):
        return 'transportation_impact'
    if any(x in t for x in ['shadow study', 'sun shadow', 'shadow analysis']):
        return 'shadow_study'
    if any(x in t for x in ['wind study', 'pedestrian level wind', 'wind analysis']):
        return 'wind_study'
    if any(x in t for x in ['noise control', 'noise impact', 'noise study', 'noise attenuation',
                              'stationary noise', 'acoustic', 'vibration study']):
        return 'noise_impact'
    if any(x in t for x in ['urban design brief', 'design brief', 'urban design guideline']):
        return 'urban_design'
    if any(x in t for x in ['heritage impact', 'cultural heritage', 'heritage assessment']):
        return 'heritage_impact'
    if any(x in t for x in ['environmental impact statement', 'environmental impact',
                              'fluvial geomorphology', 'groundwater impact']):
        return 'environmental_impact'
    if any(x in t for x in ['archaeological', 'archae']):
        return 'archaeological'
    if any(x in t for x in ['planning rationale', ' rationale']):
        return 'planning_rationale'
    if any(x in t for x in ['site plan', 'site location', 'site stats']):
        return 'site_plan'
    if any(x in t for x in ['landscape plan', 'planting plan', 'landscape']):
        return 'landscape'
    if any(x in t for x in ['geotechnical', 'geo-technical', 'hydrogeolog', 'geomorphology']):
        return 'geotechnical'
    if any(x in t for x in ['civil plan', 'civil drawing', 'civil package', 'civil set',
                              'servicing', 'serviceability', 'stormwater', 'swm',
                              'storm water', 'drainage', 'sanitary', 'water distribution',
                              'general plan of service', 'septic']):
        return 'servicing'
    if any(x in t for x in ['phase i ', 'phase 1 ', 'phase ii', 'phase 2',
                              'environmental site assessment', 'esa']):
        return 'environmental_assessment'
    if any(x in t for x in ['survey plan', 'plan of survey', 'topograph', 'survey']):
        return 'survey'
    if any(x in t for x in ['architectural', 'elevation', 'floor plan', 'rendering',
                              'perspective', 'drawings', 'canopy']):
        return 'architectural_plans'
    if any(x in t for x in ['tree conservation', 'tree preservation', 'tree permit',
                              'tree report']):
        return 'tree_conservation'
    if any(x in t for x in ['grading', 'grade control', 'erosion', 'sediment',
                              'existing conditions', 'removals plan']):
        return 'grading'
    if any(x in t for x in ['draft plan', 'plan of subdivision', 'plan of condominium']):
        return 'draft_plan'
    if any(x in t for x in ['demo plan', 'demolition plan', 'designated substance']):
        return 'demo_plan'
    if any(x in t for x in ['cover letter', 'covering letter', 'application summary',
                              'notice of decision', 'rationale letter', 'delegated authority',
                              'zoning confirmation']):
        return 'correspondence'
    return 'other'


def parse_file_size_mb(size_str):
    """Parse '0.58 MB' → 0.58, or None if unparseable."""
    if not size_str:
        return None
    m = re.search(r'([\d.]+)\s*MB', size_str, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# Schema migrations
# ---------------------------------------------------------------------------

MIGRATIONS = [
    # Enrichment columns on development_applications
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS storeys SMALLINT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS unit_count INT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS use_type TEXT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS building_type TEXT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS parking_spaces INT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS gross_floor_area_m2 INT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS planner_name TEXT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS planner_email TEXT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS ward_name TEXT",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS can_comment BOOLEAN",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS end_of_circulation_date DATE",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS devapps_fetched_at TIMESTAMPTZ",
    "ALTER TABLE development_applications ADD COLUMN IF NOT EXISTS devapps_status TEXT",
    # Documents table
    """
    CREATE TABLE IF NOT EXISTS development_application_documents (
        doc_reference_id  TEXT PRIMARY KEY,
        application_number TEXT NOT NULL,
        document_name     TEXT NOT NULL,
        file_size_mb      NUMERIC(6,2),
        file_path         TEXT,
        doc_type          TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_devapp_docs_app_number ON development_application_documents(application_number)",
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    pulled_at = datetime.now(timezone.utc)
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Apply schema migrations
    for sql in MIGRATIONS:
        cur.execute(sql)
    conn.commit()

    # --- Phase 1: ArcGIS upsert ---
    print("Phase 1: Fetching from ArcGIS...")
    total = 0
    skipped = 0
    offset = 0

    while True:
        print(f"  Fetching records {offset}–{offset + PAGE_SIZE - 1}...")
        features, has_more = fetch_arcgis_page(offset)
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
                ON CONFLICT (objectid) DO UPDATE SET
                    status       = EXCLUDED.status,
                    status_date  = EXCLUDED.status_date,
                    address      = EXCLUDED.address,
                    pulled_at    = EXCLUDED.pulled_at
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

    # Remove stale records no longer in City data
    cur.execute("DELETE FROM development_applications WHERE pulled_at < %s", [pulled_at])
    stale = cur.rowcount

    # Remove documents for deleted applications
    cur.execute("""
        DELETE FROM development_application_documents
        WHERE application_number NOT IN (
            SELECT DISTINCT application_number FROM development_applications
        )
    """)

    conn.commit()
    print(f"  {total} upserted, {skipped} skipped, {stale} stale removed.")

    # --- Phase 2: devapps enrichment (incremental) ---
    print("Phase 2: Enriching from devapps API...")

    cur.execute("""
        SELECT DISTINCT application_number
        FROM development_applications
        WHERE devapps_fetched_at IS NULL
          AND application_number IS NOT NULL
        ORDER BY application_number
    """)
    pending = [row[0] for row in cur.fetchall()]
    print(f"  {len(pending)} applications to enrich...")

    enriched = 0
    not_found = 0
    phase2_fetched = set()

    for app_number in pending:
        data = fetch_devapp(app_number)
        now = datetime.now(timezone.utc)

        if data is None:
            # Not in devapps — mark as fetched so we don't retry
            cur.execute("""
                UPDATE development_applications
                SET devapps_fetched_at = %s
                WHERE application_number = %s
            """, [now, app_number])
            not_found += 1
        else:
            desc = (data.get("applicationBriefDesc") or {}).get("en") or None
            devapps_status = (data.get("applicationStatus") or {}).get("en") or None

            # Planner info
            planner_first = (data.get("plannerFirstName") or "").strip()
            planner_last = (data.get("plannerLastName") or "").strip()
            planner_name = " ".join(filter(None, [planner_first, planner_last])) or None
            planner_email = (data.get("plannerEmail") or "").strip() or None

            # Ward
            ward = data.get("devAppWard") or {}
            ward_name = (ward.get("en") or "").strip() or None

            # Comment period
            can_comment = data.get("canComment")
            eoc_raw = data.get("endOfCirculationDateYMD") or ""
            end_of_circ = None
            if eoc_raw:
                try:
                    end_of_circ = datetime.strptime(eoc_raw, "%Y-%m-%d").date()
                except ValueError:
                    pass

            cur.execute("""
                UPDATE development_applications SET
                    description            = %s,
                    storeys                = %s,
                    unit_count             = %s,
                    use_type               = %s,
                    building_type          = %s,
                    parking_spaces         = %s,
                    gross_floor_area_m2    = %s,
                    planner_name           = %s,
                    planner_email          = %s,
                    ward_name              = %s,
                    can_comment            = %s,
                    end_of_circulation_date = %s,
                    devapps_status         = %s,
                    devapps_fetched_at     = %s
                WHERE application_number = %s
            """, [
                desc,
                extract_storeys(desc),
                extract_units(desc),
                extract_use_type(desc),
                extract_building_type(desc),
                extract_parking(desc),
                extract_gfa(desc),
                planner_name,
                planner_email,
                ward_name,
                can_comment,
                end_of_circ,
                devapps_status,
                now,
                app_number,
            ])

            # Documents
            for doc in data.get("devAppDocuments") or []:
                ref_id = doc.get("docReferenceId")
                if not ref_id:
                    continue
                cur.execute("""
                    INSERT INTO development_application_documents
                        (doc_reference_id, application_number, document_name,
                         file_size_mb, file_path, doc_type)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (doc_reference_id) DO NOTHING
                """, [
                    ref_id,
                    app_number,
                    doc.get("documentName") or "",
                    parse_file_size_mb(doc.get("fileSize")),
                    doc.get("filePath") or None,
                    infer_doc_type(doc.get("documentName") or ""),
                ])

            enriched += 1
            phase2_fetched.add(app_number)

        conn.commit()
        time.sleep(DEVAPPS_SLEEP)

    print(f"  {enriched} enriched, {not_found} not found in devapps.")

    # --- Phase 3: backfill devapps_status for already-enriched records ---
    cur.execute("""
        SELECT DISTINCT application_number
        FROM development_applications
        WHERE devapps_fetched_at IS NOT NULL
          AND application_number IS NOT NULL
        ORDER BY application_number
    """)
    status_pending = [row[0] for row in cur.fetchall() if row[0] not in phase2_fetched]
    print(f"Phase 3: Refreshing status for {len(status_pending)} enriched records...")

    status_updated = 0
    for app_number in status_pending:
        data = fetch_devapp(app_number)
        if data is not None:
            devapps_status = (data.get("applicationStatus") or {}).get("en") or None
            cur.execute("""
                UPDATE development_applications
                SET devapps_status = %s
                WHERE application_number = %s
            """, [devapps_status, app_number])
            status_updated += 1
        conn.commit()
        time.sleep(DEVAPPS_SLEEP)

    cur.close()
    conn.close()
    print(f"  {status_updated} statuses synced.")
    print("Done.")


if __name__ == "__main__":
    run()
