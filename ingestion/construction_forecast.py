"""
City of Ottawa Construction Forecast ingestion.

Fetches both layers from the City's ArcGIS REST API:
  Layer 0 — Linear construction (road resurfacing, watermain, etc.)
  Layer 1 — Localized construction (facilities, parks, bridges)

Full refresh on each run — small dataset (~2,500 records total).

Usage:
    python -m ingestion.construction_forecast
"""

import json
import os
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

BASE_URL = "https://maps.ottawa.ca/arcgis/rest/services/ConstructionForecastData/MapServer"
LAYERS = [(0, "linear"), (1, "localized")]


def fetch_layer(layer_id):
    resp = requests.get(
        f"{BASE_URL}/{layer_id}/query",
        params={
            "where": "1=1",
            "outFields": "OBJECTID,FEATURE_TYPE,STATUS,TARGETED_START,PROJECTWEBPAGE",
            "outSR": "4326",
            "f": "geojson",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("features", [])


def run():
    pulled_at = datetime.now(timezone.utc)
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE construction_forecast")

    total = 0
    for layer_id, layer_name in LAYERS:
        print(f"Fetching construction forecast: {layer_name} layer...")
        features = fetch_layer(layer_id)
        print(f"  {len(features)} records")

        for f in features:
            props = f.get("properties") or {}
            geom = f.get("geometry")
            if not geom:
                continue

            cur.execute("""
                INSERT INTO construction_forecast
                    (objectid, layer, feature_type, status, targeted_start, project_webpage, geometry, pulled_at)
                VALUES (%s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s)
                ON CONFLICT (objectid, layer) DO UPDATE SET
                    feature_type    = EXCLUDED.feature_type,
                    status          = EXCLUDED.status,
                    targeted_start  = EXCLUDED.targeted_start,
                    project_webpage = EXCLUDED.project_webpage,
                    geometry        = EXCLUDED.geometry,
                    pulled_at       = EXCLUDED.pulled_at
            """, [
                props.get("OBJECTID"),
                layer_name,
                (props.get("FEATURE_TYPE") or "").strip() or None,
                (props.get("STATUS") or "").strip() or None,
                (props.get("TARGETED_START") or "").strip() or None,
                (props.get("PROJECTWEBPAGE") or "").strip() or None,
                json.dumps(geom),
                pulled_at,
            ])
            total += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"Construction forecast: {total} records saved.")


if __name__ == "__main__":
    run()
