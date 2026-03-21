"""
TrafficScore API — FastAPI backend. v1.0

Endpoints:
  GET /api/segments          GeoJSON of all scored segments (for map)
  GET /api/segments/{id}     Detail + score breakdown for one segment
  GET /api/segments/nearby   Segments near a lat/lng point
"""

import json
import os
import re

import psycopg2
import requests as http_requests
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

app = FastAPI(title="TrafficScore API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


@app.get("/api/segments")
def get_segments(
    min_lon: float = Query(-76.3554),
    min_lat: float = Query(45.1189),
    max_lon: float = Query(-75.2462),
    max_lat: float = Query(45.5376),
    road_class: str = Query(None, description="Filter by road class (e.g. residential)"),
    max_composite: float = Query(None, description="Only return segments with score below this"),
):
    """
    Return scored road segments as GeoJSON for map rendering.
    Filtered by bounding box. Defaults to full Ottawa area.
    """
    conn = get_conn()
    cur = conn.cursor()

    where = [
        "rs.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)",
        "ss.composite_score IS NOT NULL",
    ]
    params = [min_lon, min_lat, max_lon, max_lat]

    if road_class:
        where.append("rs.road_class = %s")
        params.append(road_class)

    if max_composite is not None:
        where.append("ss.composite_score <= %s")
        params.append(max_composite)

    query = f"""
        SELECT
            rs.id,
            rs.name,
            rs.road_class,
            rs.speed_limit,
            rs.cutthrough_risk,
            ss.volume_score,
            ss.speed_score,
            ss.safety_score,
            ss.cutthrough_score,
            ss.trend_score,
            ss.composite_score,
            ST_AsGeoJSON(rs.geometry) AS geometry
        FROM road_segments rs
        JOIN street_scores ss ON ss.segment_id = rs.id
        WHERE {' AND '.join(where)}
        LIMIT 50000
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    features = []
    for row in rows:
        geom = json.loads(row.pop("geometry"))
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": dict(row),
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "count": len(features),
    }


@app.get("/api/segments/nearby")
def get_nearby(
    lat: float = Query(...),
    lng: float = Query(...),
    radius_m: int = Query(500, description="Search radius in metres"),
):
    """Return scored segments within radius_m metres of a point."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            rs.id,
            rs.name,
            rs.road_class,
            rs.speed_limit,
            ss.volume_score,
            ss.speed_score,
            ss.safety_score,
            ss.cutthrough_score,
            ss.trend_score,
            ss.composite_score,
            ST_Distance(rs.geometry::geography, ST_MakePoint(%s, %s)::geography) AS distance_m,
            ST_AsGeoJSON(rs.geometry) AS geometry
        FROM road_segments rs
        JOIN street_scores ss ON ss.segment_id = rs.id
        WHERE ST_DWithin(
            rs.geometry::geography,
            ST_MakePoint(%s, %s)::geography,
            %s
        )
        ORDER BY distance_m
        LIMIT 100
    """, [lng, lat, lng, lat, radius_m])

    rows = cur.fetchall()
    cur.close()
    conn.close()

    features = []
    for row in rows:
        geom = json.loads(row.pop("geometry"))
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": dict(row),
        })

    return {"type": "FeatureCollection", "features": features}


@app.get("/api/neighbourhood")
def get_neighbourhood(
    lat: float = Query(...),
    lng: float = Query(...),
    radius_m: int = Query(600),
):
    """Aggregate score for residential streets near a point."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            ROUND(AVG(ss.composite_score)::numeric, 1) AS avg_score,
            COUNT(*) AS num_segments
        FROM road_segments rs
        JOIN street_scores ss ON ss.segment_id = rs.id
        WHERE rs.road_class IN ('residential', 'unclassified', 'living_street')
          AND ST_DWithin(rs.geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
          AND ss.composite_score IS NOT NULL
    """, [lng, lat, radius_m])

    row = cur.fetchone()
    cur.close()
    conn.close()

    return {"avg_score": row["avg_score"], "num_segments": row["num_segments"]}


@app.get("/api/segments/{segment_id}")
def get_segment(segment_id: int):
    """Return full detail and score breakdown for a single segment."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            rs.id,
            rs.name,
            rs.road_class,
            rs.speed_limit,
            rs.lanes,
            rs.oneway,
            rs.cutthrough_risk,
            ss.volume_score,
            ss.speed_score,
            ss.safety_score,
            ss.cutthrough_score,
            ss.trend_score,
            ss.composite_score,
            ss.computed_at,
            ST_AsGeoJSON(rs.geometry) AS geometry
        FROM road_segments rs
        LEFT JOIN street_scores ss ON ss.segment_id = rs.id
        WHERE rs.id = %s
    """, [segment_id])

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Segment not found")

    geom = row.pop("geometry")
    return {
        "type": "Feature",
        "geometry": geom,
        "properties": dict(row),
    }


_ABBREVS = [
    (r"\bave\b",  "avenue"),
    (r"\bblvd\b", "boulevard"),
    (r"\bcres\b", "crescent"),
    (r"\bcrt\b",  "court"),
    (r"\bct\b",   "court"),
    (r"\bdr\b",   "drive"),
    (r"\bpkwy\b", "parkway"),
    (r"\bpl\b",   "place"),
    (r"\brd\b",   "road"),
    (r"\bst\b",   "street"),
    (r"\b n\b",   " north"),
    (r"\b s\b",   " south"),
    (r"\b e\b",   " east"),
    (r"\b w\b",   " west"),
]

def _expand(q: str) -> str:
    q = q.strip().lower()
    for pattern, replacement in _ABBREVS:
        q = re.sub(pattern, replacement, q, flags=re.IGNORECASE)
    return q


@app.get("/api/search")
def search_streets(name: str = Query(..., min_length=2)):
    """Find streets by name and return their scored segments for map navigation."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            rs.id,
            rs.name,
            rs.road_class,
            rs.speed_limit,
            rs.cutthrough_risk,
            ss.volume_score,
            ss.speed_score,
            ss.safety_score,
            ss.cutthrough_score,
            ss.trend_score,
            ss.composite_score,
            ST_X(ST_Centroid(rs.geometry)) AS lon,
            ST_Y(ST_Centroid(rs.geometry)) AS lat,
            ST_AsGeoJSON(rs.geometry) AS geometry
        FROM road_segments rs
        JOIN street_scores ss ON ss.segment_id = rs.id
        WHERE rs.name ILIKE %s
          AND ss.composite_score IS NOT NULL
        ORDER BY rs.name, ss.composite_score DESC
        LIMIT 200
    """, [f"%{_expand(name)}%"])

    rows = cur.fetchall()
    cur.close()
    conn.close()

    features = []
    for row in rows:
        geom = json.loads(row.pop("geometry"))
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": dict(row),
        })

    return {"type": "FeatureCollection", "features": features, "count": len(features)}


@app.get("/api/reverse-geocode")
def reverse_geocode(lat: float = Query(...), lng: float = Query(...)):
    """Proxy Nominatim reverse geocoding — returns suburb/neighbourhood name."""
    resp = http_requests.get(
        "https://nominatim.openstreetmap.org/reverse",
        params={"lat": lat, "lon": lng, "format": "json"},
        headers={"User-Agent": "TrafficScoreOttawa/1.0"},
        timeout=10,
    )
    data = resp.json()
    addr = data.get("address", {})
    neighbourhood = addr.get("suburb") or addr.get("neighbourhood") or addr.get("quarter") or addr.get("city_district")
    return {"neighbourhood": neighbourhood}


@app.get("/api/geocode")
def geocode(q: str = Query(..., min_length=3)):
    """Proxy Nominatim address geocoding (Ottawa area)."""
    resp = http_requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={
            "q": q + ", Ottawa, Ontario, Canada",
            "format": "json",
            "limit": 1,
            "countrycodes": "ca",
        },
        headers={"User-Agent": "TrafficScoreOttawa/1.0"},
        timeout=10,
    )
    return resp.json()


# Serve frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
