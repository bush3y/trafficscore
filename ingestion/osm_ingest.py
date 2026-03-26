"""
OpenStreetMap road network ingestion for Ottawa.

Uses osmnx (which handles Overpass API pagination/retries automatically)
to fetch the Ottawa road network and load it into road_segments.

Fetches: all driveable road types including motorways and trunk roads.
Motorways/trunk are included so that TomTom probe data and collision records
from those roads snap to the correct segment rather than bleeding onto adjacent
residential streets.

Usage:
    python -m ingestion.osm_ingest
"""

import hashlib
import os

import osmnx as ox
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from shapely.geometry import mapping

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

# Road classes to include.
# Motorways and trunk roads are included so TomTom probe data and collision
# records from those roads snap to the correct segment rather than bleeding
# onto adjacent residential streets.
TARGET_HIGHWAY_TYPES = [
    "motorway",
    "motorway_link",
    "trunk",
    "trunk_link",
    "primary",
    "primary_link",
    "secondary",
    "secondary_link",
    "tertiary",
    "tertiary_link",
    "unclassified",
    "residential",
    "living_street",
]

# Ottawa bounding box: north, south, east, west (osmnx format)
OTTAWA_BBOX = (45.5376, 45.1189, -75.2462, -76.3554)


def fetch_osm():
    print("Fetching Ottawa road network via osmnx (this may take a few minutes)...")
    ox.settings.log_console = False
    ox.settings.useful_tags_way = [
        "name", "highway", "maxspeed", "lanes", "oneway", "surface"
    ]

    custom_filter = (
        '["highway"~"'
        + "|".join(TARGET_HIGHWAY_TYPES)
        + '"]'
    )

    north, south, east, west = OTTAWA_BBOX
    G = ox.graph_from_bbox(
        bbox=(west, south, east, north),  # osmnx 2.x: (left, bottom, right, top)
        network_type="drive",
        custom_filter=custom_filter,
        retain_all=True,
    )

    # Convert to GeoDataFrame of edges (road segments)
    _, edges = ox.graph_to_gdfs(G)
    print(f"Fetched {len(edges)} road segments")
    return edges


def parse_edges(edges) -> list[dict]:
    segments = []
    seen_edges = set()

    for (u, v, key), row in edges.iterrows():
        # Deduplicate by undirected edge — prevents storing both directions of
        # a two-way street while keeping every block of a long road (the old
        # osmid-based dedup only kept ONE block per OSM way, leaving gaps).
        edge_key = (min(u, v), max(u, v), key)
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

        # Stable 56-bit integer ID from (u, v, key) — fits in BIGINT
        h = hashlib.md5(f"{min(u,v)},{max(u,v)},{key}".encode()).digest()
        edge_id = int.from_bytes(h[:7], "big")

        speed_limit = None
        raw_speed = str(row.get("maxspeed", "") or "")
        # Handle "50 mph", "50 km/h", "50" formats
        digits = "".join(c for c in raw_speed.split()[0] if c.isdigit())
        if digits:
            speed_limit = int(digits)

        lanes = None
        raw_lanes = str(row.get("lanes", "") or "")
        if raw_lanes.isdigit():
            lanes = int(raw_lanes)

        oneway = str(row.get("oneway", "False")).lower() in ("true", "yes", "1")

        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        # WKT for PostGIS
        coords = list(geom.coords)
        coord_str = ", ".join(f"{lon} {lat}" for lon, lat in coords)
        wkt = f"LINESTRING({coord_str})"

        segments.append({
            "id": edge_id,
            "name": row.get("name") if isinstance(row.get("name"), str) else None,
            "road_class": row.get("highway") if isinstance(row.get("highway"), str) else str(row.get("highway", "")),
            "speed_limit": speed_limit,
            "lanes": lanes,
            "oneway": oneway,
            "wkt": wkt,
        })

    print(f"Parsed {len(segments)} unique road segments")
    return segments


def load_segments(segments: list[dict], conn):
    rows = [
        (
            s["id"],
            s["name"],
            s["road_class"],
            s["speed_limit"],
            s["lanes"],
            s["oneway"],
            s["wkt"],
        )
        for s in segments
    ]

    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO road_segments (id, name, road_class, speed_limit, lanes, oneway, geometry)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            road_class = EXCLUDED.road_class,
            speed_limit = EXCLUDED.speed_limit,
            lanes      = EXCLUDED.lanes,
            oneway     = EXCLUDED.oneway,
            geometry   = EXCLUDED.geometry
        """,
        rows,
        template="(%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))",
    )
    conn.commit()
    cur.close()
    print(f"Loaded {len(rows)} road segments into road_segments")


def run():
    edges = fetch_osm()
    segments = parse_edges(edges)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        load_segments(segments, conn)
    finally:
        conn.close()

    print("OSM ingestion complete.")


if __name__ == "__main__":
    run()
