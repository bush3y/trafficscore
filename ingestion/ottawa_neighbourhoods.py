"""
Ottawa Neighbourhood Boundaries ingestion.

Downloads ONS Gen 3 neighbourhood boundary polygons from Ottawa Open Data
and loads them into PostGIS. Run this once (or to refresh boundaries).

Usage:
    python -m ingestion.ottawa_neighbourhoods
"""

import json
import os

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

GEOJSON_URL = (
    "https://services.arcgis.com/G6F8XLCl5KtAlZ2G/arcgis/rest/services"
    "/GEN3_OTT_1_3_3/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
)


def run():
    print("Fetching Ottawa neighbourhood boundaries from Ottawa Open Data...")
    resp = requests.get(GEOJSON_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    print(f"  {len(features)} neighbourhoods downloaded")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE neighbourhoods RESTART IDENTITY")

    inserted = 0
    for f in features:
        name = f["properties"]["ONS_Name"].title()
        geom = f["geometry"]

        # ArcGIS sometimes returns Polygon — normalise to MultiPolygon
        if geom["type"] == "Polygon":
            geom = {"type": "MultiPolygon", "coordinates": [geom["coordinates"]]}

        cur.execute(
            """
            INSERT INTO neighbourhoods (name, geometry)
            VALUES (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            """,
            [name, json.dumps(geom)],
        )
        inserted += 1

    conn.commit()
    print(f"  {inserted} neighbourhoods inserted")
    cur.close()
    conn.close()


if __name__ == "__main__":
    run()
