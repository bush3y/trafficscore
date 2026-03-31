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
from fastapi.responses import FileResponse
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
    """Aggregate score for residential streets — uses neighbourhood polygon if available, else radius."""
    conn = get_conn()
    cur = conn.cursor()

    # Try neighbourhood polygon first
    cur.execute("""
        SELECT id FROM neighbourhoods
        WHERE ST_Contains(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
    """, [lng, lat])
    nb = cur.fetchone()

    if nb:
        cur.execute("""
            SELECT
                ROUND(AVG(ss.composite_score)::numeric, 1) AS avg_score,
                COUNT(*) AS num_segments
            FROM road_segments rs
            JOIN street_scores ss ON ss.segment_id = rs.id
            WHERE rs.road_class IN ('residential', 'unclassified', 'living_street')
              AND ST_Contains(
                    (SELECT geometry FROM neighbourhoods WHERE id = %s),
                    ST_Centroid(rs.geometry)
                  )
              AND ss.composite_score IS NOT NULL
        """, [nb["id"]])
    else:
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


@app.get("/api/relative")
def get_relative(
    road_class: str = Query(...),
    score: float = Query(...),
    lat: float = Query(...),
    lng: float = Query(...),
    radius_m: int = Query(600),
):
    """Percentile rank within road class — city-wide and neighbourhood."""
    conn = get_conn()
    cur = conn.cursor()

    # City-wide: how many same-class segments score <= this one
    cur.execute("""
        SELECT
            ROUND(
                (COUNT(*) FILTER (WHERE ss.composite_score <= %s))::numeric /
                NULLIF(COUNT(*), 0) * 100
            )::int AS percentile,
            COUNT(*) AS total
        FROM road_segments rs
        JOIN street_scores ss ON ss.segment_id = rs.id
        WHERE rs.road_class = %s AND ss.composite_score IS NOT NULL
    """, [score, road_class])
    city = cur.fetchone()

    # Neighbourhood: distinct street names within radius, avg score per street
    cur.execute("""
        WITH streets AS (
            SELECT
                COALESCE(rs.name, rs.id::text) AS street_name,
                AVG(ss.composite_score) AS avg_score
            FROM road_segments rs
            JOIN street_scores ss ON ss.segment_id = rs.id
            WHERE rs.road_class = %s
              AND ST_DWithin(rs.geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
              AND ss.composite_score IS NOT NULL
            GROUP BY street_name
        )
        SELECT
            ROUND(
                (COUNT(*) FILTER (WHERE avg_score <= %s))::numeric /
                NULLIF(COUNT(*), 0) * 100
            )::int AS percentile,
            COUNT(*) AS total
        FROM streets
    """, [road_class, lng, lat, radius_m, score])
    nb = cur.fetchone()

    cur.close()
    conn.close()

    return {
        "road_class": road_class,
        "city_percentile": city["percentile"] if city else None,
        "city_total": int(city["total"]) if city else None,
        "nb_percentile": nb["percentile"] if nb else None,
        "nb_total": int(nb["total"]) if nb else None,
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
    """Returns neighbourhood name — DB polygon lookup first, Nominatim fallback."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM neighbourhoods
        WHERE ST_Contains(geometry, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
    """, [lng, lat])
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return {"neighbourhood": row["name"]}

    # Fall back to Nominatim for areas outside ONS boundaries (rural Ottawa)
    resp = http_requests.get(
        "https://nominatim.openstreetmap.org/reverse",
        params={"lat": lat, "lon": lng, "format": "json"},
        headers={"User-Agent": "TrafficScoreOttawa/1.0"},
        timeout=10,
    )
    data = resp.json()
    addr = data.get("address", {})
    neighbourhood = (addr.get("suburb") or addr.get("neighbourhood") or
                     addr.get("quarter") or addr.get("village") or
                     addr.get("hamlet") or addr.get("city_district"))
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


@app.get("/api/bus-routes")
def get_bus_routes(lat: float = Query(...), lng: float = Query(...)):
    """Bus routes passing within ~30m of a point, ordered by frequency."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT route_name, SUM(weekday_trips) AS weekday_trips
        FROM bus_routes
        WHERE ST_DWithin(
            geometry::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            30
        )
        GROUP BY route_name
        ORDER BY weekday_trips DESC
    """, [lng, lat])
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return {"routes": rows}


@app.get("/api/admin/status")
def admin_status():
    """Data freshness stats for the admin dashboard."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS count FROM road_segments")
    road_segments = dict(cur.fetchone())

    cur.execute("""
        SELECT COUNT(*) AS count,
               MIN(date_from)::text AS date_from,
               MAX(date_to)::text   AS date_to,
               MAX(pulled_at)::text AS last_pulled
        FROM tomtom_segments
    """)
    tomtom = dict(cur.fetchone())

    cur.execute("""
        SELECT COUNT(*) AS count,
               MIN(year) AS year_from,
               MAX(year) AS year_to
        FROM collisions
    """)
    collisions = dict(cur.fetchone())

    cur.execute("""
        SELECT COUNT(*) AS count,
               MIN(year) AS year_from,
               MAX(year) AS year_to
        FROM intersection_volumes
    """)
    intersection_volumes = dict(cur.fetchone())

    cur.execute("""
        SELECT COUNT(*) AS total,
               COUNT(composite_score) AS scored,
               MAX(computed_at)::text AS last_computed
        FROM street_scores
    """)
    scores = dict(cur.fetchone())

    cur.execute("SELECT COUNT(*) AS count FROM neighbourhoods")
    neighbourhoods = dict(cur.fetchone())

    cur.execute("""
        SELECT COUNT(*) AS count,
               MAX(fetched_at)::text AS last_fetched,
               MIN(schedule_start)::text AS schedule_start,
               MAX(schedule_end)::text AS schedule_end
        FROM bus_routes
    """)
    bus_routes = dict(cur.fetchone())

    cur.execute("""
        SELECT COUNT(*) AS count, MAX(pulled_at)::text AS last_pulled
        FROM construction_forecast
    """)
    construction_forecast = dict(cur.fetchone())

    cur.execute("""
        SELECT
            COUNT(*)                                                        AS count,
            MAX(pulled_at)::text                                            AS last_pulled,
            COUNT(*) FILTER (WHERE description IS NOT NULL)                 AS enriched,
            COUNT(*) FILTER (WHERE storeys IS NOT NULL)                     AS with_storeys,
            COUNT(*) FILTER (WHERE unit_count IS NOT NULL)                  AS with_units,
            (SELECT COUNT(*) FROM development_application_documents)        AS document_count
        FROM development_applications
    """)
    development_applications = dict(cur.fetchone())

    cur.close()
    conn.close()

    return {
        "road_segments": road_segments,
        "tomtom": tomtom,
        "collisions": collisions,
        "intersection_volumes": intersection_volumes,
        "scores": scores,
        "neighbourhoods": neighbourhoods,
        "bus_routes": bus_routes,
        "construction_forecast": construction_forecast,
        "development_applications": development_applications,
    }


@app.get("/api/development-activity")
def get_development_activity(
    lat: float = Query(...),
    lng: float = Query(...),
    radius_m: int = Query(500),
    dev_radius_m: int = Query(750),
):
    """City construction projects and development applications near a point."""
    conn = get_conn()
    cur = conn.cursor()

    # Feature types that represent street-level disruption relevant to home buyers.
    # Excludes facilities/parks/transit-infrastructure (OBLDG, OPARKS, GNP, etc.).
    # GNT (New Transit) included only when project_webpage is set — proxy for major projects (e.g. LRT).
    cur.execute("""
        WITH relevant AS (
            SELECT
                feature_type,
                status,
                targeted_start,
                project_webpage,
                traffic_impacts,
                geometry,
                COALESCE(NULLIF(project_webpage, ''), objectid::text) AS project_key
            FROM construction_forecast
            WHERE ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
              AND (
                feature_type IN (
                  'RRSW', 'RD_RESF', 'RD_SURF', 'RD_SWRE', 'RD_CS',
                  'RS', 'RSL', 'RWM', 'SCR', 'SWM', 'SBR', 'WBO',
                  'MIM', 'GNR', 'MS', 'RSS', 'CREN', 'RD_MUPR'
                )
                OR (feature_type = 'GNT' AND project_webpage IS NOT NULL AND project_webpage != '')
              )
        ),
        deduped AS (
            SELECT DISTINCT ON (project_key)
                feature_type,
                status,
                targeted_start,
                project_webpage,
                CASE WHEN traffic_impacts ~* '^\s*(none|no[\s.]|no$|n/?a|na)\s*'
                     THEN NULL ELSE traffic_impacts END AS traffic_impacts,
                ROUND(ST_Distance(geometry::geography, ST_MakePoint(%s, %s)::geography)::numeric) AS distance_m,
                COUNT(*) OVER (PARTITION BY project_key) AS segment_count,
                ROUND(SUM(ST_Length(geometry::geography)) OVER (PARTITION BY project_key)::numeric) AS total_length_m
            FROM relevant
            ORDER BY project_key, ST_Distance(geometry::geography, ST_MakePoint(%s, %s)::geography)
        )
        SELECT * FROM deduped
        ORDER BY distance_m
        LIMIT 10
    """, [lng, lat, radius_m, lng, lat, lng, lat])
    construction = [dict(r) for r in cur.fetchall()]

    # Completed/terminal statuses excluded — only active/pending applications shown.
    # "Agreement Registered - Securities Held" kept: developer obligations still outstanding.
    # "Application Approved by OMB" kept: bylaw enactment and construction still pending.
    # Null status excluded: unknown state.
    cur.execute("""
        SELECT
            application_number,
            application_type,
            status,
            status_date::text AS status_date,
            address,
            storeys,
            unit_count,
            use_type,
            building_type,
            parking_spaces,
            gross_floor_area_m2,
            devapps_status IS NOT NULL AS in_devapps,
            ROUND(ST_Distance(geometry::geography, ST_MakePoint(%s, %s)::geography)::numeric) AS distance_m
        FROM development_applications
        WHERE ST_DWithin(geometry::geography, ST_MakePoint(%s, %s)::geography, %s)
          AND status IS NOT NULL
          AND application_type != 'Zoning By-law Amendment'
          AND devapps_status IN ('Active', 'File Pending', 'Post Approval')
          AND status NOT ILIKE '%%in effect%%'
          AND status NOT IN (
            'Agreement Registered - Final Legal Clearance Given',
            'No Appeal - Official Plan Amendment Adopted',
            'OMB Appeal Withdrawn - Application Approved',
            'Application Refused by OMB',
            'By-law Refused',
            'Approval Lapsed - No Building Permit Issued',
            'Agreement lapsed',
            'Application Approved - No Agreement/Letter of Undertaking Required',
            'Application Approved: No Agreement/Letter of Undertaking Required',
            'Approved - Agreement Signed, Registration Not Required',
            'CWN approved'
          )
        ORDER BY distance_m
        LIMIT 10
    """, [lng, lat, lng, lat, dev_radius_m])
    development = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()

    return {"construction": construction, "development": development}


@app.get("/api/validation")
def validation():
    """Volume score vs measured traffic count correlation + outliers."""
    conn = get_conn()
    cur = conn.cursor()

    # Build matched pairs once — reused by all three queries below.
    # Name-match filter: segment's first word must appear at the start of one
    # of the two road names in the intersection_name (split on " @ ").
    # This eliminates cross-road spatial contamination (e.g. a small side road
    # matched to a count measuring the major arterial 50m away).
    cur.execute("""
        CREATE TEMP TABLE val_pairs AS
        WITH latest_volumes AS (
            SELECT DISTINCT ON (intersection_name)
                intersection_name, volume, geometry
            FROM intersection_volumes
            ORDER BY intersection_name, year DESC
        )
        SELECT DISTINCT ON (rs.id)
            rs.id,
            rs.name,
            rs.road_class,
            ss.volume_score,
            ss.composite_score,
            lv.volume            AS measured_volume,
            lv.intersection_name
        FROM latest_volumes lv
        JOIN road_segments rs ON ST_DWithin(rs.geometry, lv.geometry, 0.0005)
        JOIN street_scores ss  ON ss.segment_id = rs.id
        WHERE ss.volume_score IS NOT NULL
          AND rs.name IS NOT NULL
          AND LENGTH(split_part(rs.name, ' ', 1)) >= 3
          AND (
            LOWER(split_part(lv.intersection_name, ' @ ', 1)) LIKE LOWER(split_part(rs.name, ' ', 1)) || '%'
            OR LOWER(split_part(lv.intersection_name, ' @ ', 2)) LIKE LOWER(split_part(rs.name, ' ', 1)) || '%'
          )
        ORDER BY rs.id, ST_Distance(rs.geometry, lv.geometry)
    """)

    cur.execute("""
        CREATE TEMP TABLE val_ranked AS
        SELECT *,
            PERCENT_RANK() OVER (ORDER BY volume_score)    AS rank_score,
            PERCENT_RANK() OVER (ORDER BY measured_volume) AS rank_measured
        FROM val_pairs
    """)
    conn.commit()

    # Spearman correlation by road class
    cur.execute("""
        SELECT
            road_class,
            COUNT(*)                                                       AS pairs,
            ROUND(CORR(rank_score, rank_measured)::numeric, 3)            AS spearman,
            COUNT(*) FILTER (WHERE ABS(rank_score - rank_measured) > 0.4) AS large_mismatch,
            ROUND(AVG(volume_score)::numeric, 1)                          AS avg_vol_score,
            ROUND(AVG(measured_volume)::numeric, 0)                       AS avg_measured
        FROM val_ranked
        GROUP BY road_class
        HAVING COUNT(*) >= 10
        ORDER BY spearman DESC
    """)
    correlations = [dict(r) for r in cur.fetchall()]

    # Top outliers: scored HIGH but measured LOW (overscored).
    # Deduplicated by (name, road_class, intersection_name) — keeps the
    # segment with the largest gap when multiple OSM segments of the same
    # road match the same count location.
    cur.execute("""
        SELECT DISTINCT ON (name, road_class, intersection_name)
            id::text, name, road_class,
            ROUND(volume_score::numeric, 1)    AS volume_score,
            ROUND(composite_score::numeric, 1) AS composite_score,
            measured_volume,
            intersection_name,
            ROUND((rank_score    * 100)::numeric, 1) AS pct_rank_score,
            ROUND((rank_measured * 100)::numeric, 1) AS pct_rank_measured,
            ROUND(((rank_score - rank_measured) * 100)::numeric, 1) AS rank_gap
        FROM val_ranked
        WHERE rank_score - rank_measured > 0.4
        ORDER BY name, road_class, intersection_name, rank_gap DESC
        LIMIT 40
    """)
    overscored = [dict(r) for r in cur.fetchall()]
    overscored.sort(key=lambda r: r["rank_gap"], reverse=True)

    # Top outliers: scored LOW but measured HIGH (underscored).
    cur.execute("""
        SELECT DISTINCT ON (name, road_class, intersection_name)
            id::text, name, road_class,
            ROUND(volume_score::numeric, 1)    AS volume_score,
            ROUND(composite_score::numeric, 1) AS composite_score,
            measured_volume,
            intersection_name,
            ROUND((rank_score    * 100)::numeric, 1) AS pct_rank_score,
            ROUND((rank_measured * 100)::numeric, 1) AS pct_rank_measured,
            ROUND(((rank_measured - rank_score) * 100)::numeric, 1) AS rank_gap
        FROM val_ranked
        WHERE rank_measured - rank_score > 0.4
        ORDER BY name, road_class, intersection_name, rank_gap DESC
        LIMIT 40
    """)
    underscored = [dict(r) for r in cur.fetchall()]
    underscored.sort(key=lambda r: r["rank_gap"], reverse=True)

    # Safety cross-validation: compare 2019-2022 collision density vs 2023-2024 collision density
    # Streets that were high-safety in early period should still be high in later period
    cur.execute("""
        WITH nearest_segment AS (
            SELECT DISTINCT ON (c.id)
                c.id         AS collision_id,
                rs.id        AS segment_id,
                rs.name,
                rs.road_class,
                c.year
            FROM collisions c
            JOIN road_segments rs ON ST_DWithin(rs.geometry, c.geometry, 0.000135)
            ORDER BY c.id, c.geometry <-> rs.geometry
        ),
        lengths AS (
            SELECT id, GREATEST(ST_Length(geometry::geography) / 1000.0, 0.05) AS km
            FROM road_segments
        ),
        early AS (
            SELECT segment_id, COUNT(*) AS hits
            FROM nearest_segment WHERE year BETWEEN 2019 AND 2021 GROUP BY segment_id
        ),
        late AS (
            SELECT segment_id, COUNT(*) AS hits
            FROM nearest_segment WHERE year BETWEEN 2022 AND 2024 GROUP BY segment_id
        )
        SELECT
            COUNT(*) FILTER (WHERE e.hits >= 2 AND l.hits >= 2)        AS both_active,
            COUNT(*) FILTER (WHERE e.hits >= 2 AND l.hits IS NULL)      AS went_quiet,
            COUNT(*) FILTER (WHERE e.hits IS NULL AND l.hits >= 2)      AS newly_dangerous,
            ROUND(CORR(
                COALESCE(e.hits, 0)::float / ln.km,
                COALESCE(l.hits, 0)::float / ln.km
            )::numeric, 3) AS density_correlation
        FROM road_segments rs
        JOIN lengths ln ON ln.id = rs.id
        LEFT JOIN early e ON e.segment_id = rs.id
        LEFT JOIN late  l ON l.segment_id = rs.id
        WHERE e.hits IS NOT NULL OR l.hits IS NOT NULL
    """)
    safety_xval = dict(cur.fetchone())

    cur.close()
    conn.close()

    return {
        "correlations": correlations,
        "overscored": overscored,
        "underscored": underscored,
        "safety_xval": safety_xval,
    }


@app.get("/validation")
def validation_page():
    return FileResponse("frontend/validation.html")


@app.get("/admin")
def admin_page():
    return FileResponse("frontend/admin.html")


# Serve frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
