"""
Composite street scorer — pure SQL approach.

All scoring runs directly in PostgreSQL using window functions,
avoiding slow Python/DB round-trips over large datasets.

Steps:
  1. Spatially match TomTom segments → OSM road_segments
  2. Compute volume + speed scores via PERCENT_RANK within road class
  3. Compute safety score (collision density per km)
  4. Compute trend score (2019 vs 2024 intersection volumes nearby)
  5. Write composite scores to street_scores

Usage:
    python -m scoring.scorer
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

# Score weights — must sum to 1.0
WEIGHTS = {
    "volume":     0.45,
    "speed":      0.15,
    "safety":     0.20,
    "cutthrough": 0.20,
}


def run():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ------------------------------------------------------------------
    # Step 1: Spatially match TomTom segments to OSM road_segments
    # Uses geometry (degrees) not geography — ~10x faster for small radii
    # 0.001 degrees ≈ ~80m at Ottawa's latitude, close enough for matching
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Road name matching helper
    # Compares road names word-by-word, prefix-tolerant at each position.
    # Handles abbreviations ("Ave"↔"Avenue", "St"↔"Street", "N"↔"North")
    # without false-matching different roads ("Island Park Dr" ≠ "Island Park Crescent").
    # Rule: for each word position up to min(len(a), len(b)), one word must
    # be a prefix of the other. Positions beyond the shorter name are ignored.
    # ------------------------------------------------------------------
    cur.execute("""
        CREATE OR REPLACE FUNCTION road_names_match(name1 text, name2 text)
        RETURNS boolean AS $func$
        DECLARE
            w1 text[];
            w2 text[];
            n  int;
            i  int;
            a  text;
            b  text;
        BEGIN
            IF name1 IS NULL OR name2 IS NULL THEN RETURN FALSE; END IF;
            w1 := string_to_array(lower(name1), ' ');
            w2 := string_to_array(lower(name2), ' ');
            n  := LEAST(array_length(w1, 1), array_length(w2, 1));
            FOR i IN 1..n LOOP
                a := w1[i]; b := w2[i];
                -- One word must be a prefix of the other
                IF NOT (b LIKE a || '%' OR a LIKE b || '%') THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            RETURN TRUE;
        END;
        $func$ LANGUAGE plpgsql IMMUTABLE;
    """)
    conn.commit()

    print("Step 1: Matching TomTom segments to OSM road network...")
    cur.execute("UPDATE tomtom_segments SET segment_id = NULL")
    cur.execute("""
        UPDATE tomtom_segments ts
        SET segment_id = closest.id
        FROM (
            SELECT DISTINCT ON (ts2.id)
                ts2.id   AS tomtom_row_id,
                rs.id    AS id
            FROM tomtom_segments ts2
            JOIN road_segments rs
              ON ST_DWithin(ts2.geometry, rs.geometry, 0.001)
            WHERE ts2.geometry IS NOT NULL
            ORDER BY ts2.id,
                -- Prefer same-name OSM segments to avoid cross-road contamination
                -- (e.g. Kirkwood Ave TomTom data going to an adjacent Iona St OSM segment)
                CASE WHEN road_names_match(ts2.road_name, rs.name) THEN 0 ELSE 1 END,
                ST_Distance(ts2.geometry, rs.geometry)
        ) closest
        WHERE ts.id = closest.tomtom_row_id
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM tomtom_segments WHERE segment_id IS NOT NULL")
    matched = cur.fetchone()[0]
    print(f"  {matched:,} TomTom segments matched to OSM segments")

    # ------------------------------------------------------------------
    # Step 2: Volume + speed scores
    # PERCENT_RANK within FRC class → 0–100 score
    # Higher probe count = more traffic = higher (worse) score
    # Higher speed exceedance = faster = higher (worse) score
    # ------------------------------------------------------------------
    print("Step 2: Computing volume and speed scores...")

    # Precompute name-match lookups as indexed temp tables so road_names_match()
    # is called once per candidate pair — never inside the hot ST_DWithin spatial join.
    #
    # tmp_tomtom_named: TomTom row IDs whose road_name matches their assigned OSM segment.
    #   Used to filter direct_match rows without calling road_names_match inline.
    # tmp_direct_has_name: segment IDs that have at least one named TomTom direct match.
    #   Used to decide whether a segment must use name-only data or can fall back to any.
    # tmp_spatial_named_pairs: precomputed (segment_id, probes, p85) for spatial fallback
    #   name-matched rows. Avoids calling road_names_match inside the spatial join.
    # tmp_spatial_has_name: segment IDs that have at least one named TomTom within 80m.
    #   Used to route spatial fallback segments to name-only vs any-data path.
    cur.execute("""
        CREATE TEMP TABLE tmp_tomtom_named AS
        SELECT ts.id AS tomtom_id
        FROM tomtom_segments ts
        JOIN road_segments rs ON rs.id = ts.segment_id
        WHERE ts.time_set = 'all_day'
          AND ts.probe_count >= 50
          AND road_names_match(ts.road_name, rs.name)
    """)
    cur.execute("CREATE INDEX ON tmp_tomtom_named (tomtom_id)")

    cur.execute("""
        CREATE TEMP TABLE tmp_direct_has_name AS
        SELECT DISTINCT ts.segment_id
        FROM tomtom_segments ts
        WHERE ts.id IN (SELECT tomtom_id FROM tmp_tomtom_named)
    """)
    cur.execute("CREATE INDEX ON tmp_direct_has_name (segment_id)")

    cur.execute("""
        CREATE TEMP TABLE tmp_spatial_named_pairs AS
        SELECT rs.id AS segment_id,
               ts.probe_count,
               ts.speed_p85_kmh,
               rs.speed_limit
        FROM road_segments rs
        JOIN tomtom_segments ts ON ST_DWithin(rs.geometry, ts.geometry, 0.001)
        WHERE ts.time_set = 'all_day'
          AND ts.probe_count >= 50
          AND road_names_match(ts.road_name, rs.name)
    """)
    cur.execute("CREATE INDEX ON tmp_spatial_named_pairs (segment_id)")

    cur.execute("""
        CREATE TEMP TABLE tmp_spatial_has_name AS
        SELECT DISTINCT segment_id FROM tmp_spatial_named_pairs
    """)
    cur.execute("CREATE INDEX ON tmp_spatial_has_name (segment_id)")
    conn.commit()

    cur.execute("""
        CREATE TEMP TABLE tomtom_scores AS
        WITH direct_match AS (
            -- Primary: use TomTom segments directly matched to each OSM segment in Step 1.
            -- If same-name TomTom data exists for this segment, use ONLY that — prevents
            -- high-volume roads (e.g. The Queensway) from contaminating adjacent streets.
            -- Falls back to any-name data only when no same-name match is available,
            -- with the residential probe cap as an additional safety net.
            -- No road_names_match() calls here — uses precomputed tmp_tomtom_named index.
            SELECT
                ts.segment_id,
                AVG(ts.probe_count)   AS avg_probes,
                AVG(ts.speed_p85_kmh) AS avg_p85,
                rs.speed_limit
            FROM tomtom_segments ts
            JOIN road_segments rs ON rs.id = ts.segment_id
            WHERE ts.time_set = 'all_day'
              AND ts.probe_count >= 50
              AND (
                ts.id IN (SELECT tomtom_id FROM tmp_tomtom_named)
                OR (
                  ts.segment_id NOT IN (SELECT segment_id FROM tmp_direct_has_name)
                  AND NOT (rs.road_class IN ('residential', 'unclassified', 'living_street')
                           AND ts.probe_count > 200)
                )
              )
            GROUP BY ts.segment_id, rs.speed_limit
        ),
        spatial_fallback AS (
            -- Fallback for segments with no direct TomTom match.
            -- Split into two paths to avoid calling road_names_match inside the spatial join:
            --   Path A: segment has same-name TomTom data nearby → use precomputed named pairs
            --   Path B: no same-name data exists → spatial join with residential probe cap only
            SELECT segment_id,
                   AVG(probe_count)   AS avg_probes,
                   AVG(speed_p85_kmh) AS avg_p85,
                   MIN(speed_limit)   AS speed_limit
            FROM tmp_spatial_named_pairs
            WHERE segment_id NOT IN (SELECT segment_id FROM direct_match)
            GROUP BY segment_id
            UNION ALL
            SELECT rs.id AS segment_id,
                   AVG(ts.probe_count)   AS avg_probes,
                   AVG(ts.speed_p85_kmh) AS avg_p85,
                   rs.speed_limit
            FROM road_segments rs
            JOIN tomtom_segments ts ON ST_DWithin(rs.geometry, ts.geometry, 0.001)
            WHERE ts.time_set = 'all_day'
              AND ts.probe_count IS NOT NULL
              AND rs.id NOT IN (SELECT segment_id FROM direct_match)
              AND rs.id NOT IN (SELECT segment_id FROM tmp_spatial_has_name)
              AND NOT (rs.road_class IN ('residential', 'unclassified', 'living_street')
                       AND ts.probe_count > 200)
            GROUP BY rs.id, rs.speed_limit
        ),
        raw_scores AS (
            SELECT * FROM direct_match
            UNION ALL
            SELECT * FROM spatial_fallback
        ),
        short_segment_fix AS (
            -- Short segments (<60m) are typically intersection approaches where
            -- TomTom records near-stopped speeds that don't reflect through-road
            -- conditions. Inherit TomTom values from adjacent same-name segments
            -- (>=60m) instead — the cars were going that speed before slowing.
            SELECT
                rs.id AS segment_id,
                AVG(r2.avg_probes) AS avg_probes,
                AVG(r2.avg_p85)    AS avg_p85,
                rs.speed_limit
            FROM road_segments rs
            JOIN road_segments rs2
              ON ST_DWithin(rs.geometry, rs2.geometry, 0.00015)
              AND rs.name = rs2.name
              AND rs.road_class = rs2.road_class
              AND rs2.id != rs.id
              AND ST_Length(rs2.geometry::geography) >= 60
            JOIN raw_scores r2 ON r2.segment_id = rs2.id
            WHERE rs.name IS NOT NULL
              AND ST_Length(rs.geometry::geography) < 60
            GROUP BY rs.id, rs.speed_limit
        ),
        combined AS (
            SELECT * FROM short_segment_fix
            UNION ALL
            SELECT * FROM raw_scores
            WHERE segment_id NOT IN (SELECT segment_id FROM short_segment_fix)
        )
        SELECT
            segment_id,
            ROUND((PERCENT_RANK() OVER (
                ORDER BY avg_probes
            ) * 100)::numeric, 1) AS volume_score,
            ROUND((PERCENT_RANK() OVER (
                ORDER BY avg_p85
            ) * 100)::numeric, 1) AS speed_score
        FROM combined
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM tomtom_scores")
    print(f"  {cur.fetchone()[0]:,} segments scored for volume/speed")

    # ------------------------------------------------------------------
    # Step 3: Safety scores
    # Each collision is attributed only to its nearest road segment —
    # prevents highway/arterial collisions bleeding into adjacent residential
    # streets (e.g. 417 onramp collisions counting against a nearby dead end).
    # Normalise by segment length, then PERCENT_RANK.
    # ------------------------------------------------------------------
    print("Step 3: Computing safety scores (collision density)...")
    cur.execute("""
        CREATE TEMP TABLE safety_scores AS
        WITH nearest_segment AS (
            -- Assign each collision to its single nearest road segment within 15m.
            -- 15m captures on-road collisions but excludes motorway/highway spillover
            -- onto adjacent residential streets (motorways are not in road_segments,
            -- so without a tight radius those collisions would claim the nearest
            -- residential segment instead).
            SELECT DISTINCT ON (c.id)
                c.id AS collision_id,
                rs.id AS segment_id
            FROM collisions c
            JOIN road_segments rs ON ST_DWithin(rs.geometry::geography, c.geometry::geography, 15)
            WHERE c.year >= 2019
            ORDER BY c.id, c.geometry <-> rs.geometry
        ),
        collision_counts AS (
            SELECT
                rs.id AS segment_id,
                COUNT(n.collision_id) AS num_collisions,
                GREATEST(ST_Length(rs.geometry::geography) / 1000.0, 0.05) AS length_km
            FROM road_segments rs
            LEFT JOIN nearest_segment n ON n.segment_id = rs.id
            GROUP BY rs.id, rs.geometry
        )
        SELECT
            segment_id,
            ROUND((PERCENT_RANK() OVER (
                ORDER BY CASE WHEN num_collisions >= 3 THEN num_collisions ELSE 0 END / length_km
            ) * 100)::numeric, 1) AS safety_score
        FROM collision_counts
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM safety_scores")
    print(f"  {cur.fetchone()[0]:,} segments scored for safety")

    # ------------------------------------------------------------------
    # Step 4: Trend scores
    # Compare earliest vs latest intersection volumes within 100m
    # Positive = getting busier (worse), negative = quieter (better)
    # PERCENT_RANK of % change → 50 = neutral
    # ------------------------------------------------------------------
    print("Step 4: Computing trend scores...")
    cur.execute("""
        CREATE TEMP TABLE trend_scores AS
        WITH nearby_volumes AS (
            SELECT
                rs.id AS segment_id,
                iv.year,
                AVG(iv.volume) AS avg_vol
            FROM road_segments rs
            JOIN intersection_volumes iv
                ON ST_DWithin(rs.geometry, iv.geometry, 0.001)
            GROUP BY rs.id, iv.year
        ),
        year_bounds AS (
            SELECT
                segment_id,
                MIN(year) AS min_year,
                MAX(year) AS max_year
            FROM nearby_volumes
            GROUP BY segment_id
            HAVING MAX(year) - MIN(year) >= 2
        ),
        pct_changes AS (
            SELECT
                yb.segment_id,
                (late.avg_vol - early.avg_vol) / NULLIF(early.avg_vol, 0) * 100 AS pct_change
            FROM year_bounds yb
            JOIN nearby_volumes early ON early.segment_id = yb.segment_id AND early.year = yb.min_year
            JOIN nearby_volumes late  ON late.segment_id  = yb.segment_id AND late.year  = yb.max_year
        )
        SELECT
            segment_id,
            ROUND((PERCENT_RANK() OVER (ORDER BY pct_change) * 100)::numeric, 1) AS trend_score
        FROM pct_changes
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM trend_scores")
    print(f"  {cur.fetchone()[0]:,} segments scored for trend")

    # ------------------------------------------------------------------
    # Step 5: Write composite scores
    # ------------------------------------------------------------------
    print("Step 5: Writing composite scores...")
    wv = WEIGHTS["volume"]
    ws = WEIGHTS["speed"]
    wsa = WEIGHTS["safety"]
    wc = WEIGHTS["cutthrough"]

    cur.execute(f"""
        INSERT INTO street_scores
            (segment_id, volume_score, speed_score, safety_score,
             cutthrough_score, trend_score, composite_score)
        SELECT
            rs.id,
            ts.volume_score,
            ts.speed_score,
            ss.safety_score,
            ROUND((rs.cutthrough_risk * 100)::numeric, 1),
            tr.trend_score,
            -- Weighted composite using only available components
            ROUND(((
                COALESCE(ts.volume_score  * {wv}, 0) +
                COALESCE(ts.speed_score   * {ws}, 0) +
                COALESCE(ss.safety_score * CASE WHEN ts.volume_score IS NOT NULL THEN LEAST(1.0, ts.volume_score / 60.0) ELSE 1.0 END * {wsa}, 0) +
                COALESCE(rs.cutthrough_risk * 100 * {wc}, 0)
            ) / (
                CASE WHEN ts.volume_score  IS NOT NULL THEN {wv}  ELSE 0 END +
                CASE WHEN ts.speed_score   IS NOT NULL THEN {ws}  ELSE 0 END +
                CASE WHEN ss.safety_score  IS NOT NULL THEN {wsa} * CASE WHEN ts.volume_score IS NOT NULL THEN LEAST(1.0, ts.volume_score / 60.0) ELSE 1.0 END ELSE 0 END +
                CASE WHEN rs.cutthrough_risk IS NOT NULL THEN {wc} ELSE 0 END
            ))::numeric, 1)
        FROM road_segments rs
        LEFT JOIN (
            SELECT segment_id,
                   AVG(volume_score) AS volume_score,
                   AVG(speed_score)  AS speed_score
            FROM tomtom_scores
            GROUP BY segment_id
        ) ts ON ts.segment_id = rs.id
        LEFT JOIN safety_scores  ss ON ss.segment_id = rs.id
        LEFT JOIN trend_scores   tr ON tr.segment_id = rs.id
        ON CONFLICT (segment_id) DO UPDATE SET
            volume_score     = EXCLUDED.volume_score,
            speed_score      = EXCLUDED.speed_score,
            safety_score     = EXCLUDED.safety_score,
            cutthrough_score = EXCLUDED.cutthrough_score,
            trend_score      = EXCLUDED.trend_score,
            composite_score  = EXCLUDED.composite_score,
            computed_at      = NOW()
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM street_scores WHERE composite_score IS NOT NULL")
    scored = cur.fetchone()[0]
    print(f"  {scored:,} segments with composite scores written")

    # Summary stats
    cur.execute("""
        SELECT
            road_class,
            COUNT(*) segments,
            ROUND(AVG(composite_score)::numeric,1) avg_score,
            ROUND(MIN(composite_score)::numeric,1) min_score,
            ROUND(MAX(composite_score)::numeric,1) max_score
        FROM street_scores ss
        JOIN road_segments rs ON rs.id = ss.segment_id
        WHERE composite_score IS NOT NULL
        GROUP BY road_class
        ORDER BY avg_score DESC
    """)
    print(f"\n{'Road class':<20} {'Segments':<10} {'Avg':<8} {'Min':<8} {'Max'}")
    for row in cur.fetchall():
        print(f"{str(row[0]):<20} {row[1]:<10} {row[2]:<8} {row[3]:<8} {row[4]}")

    cur.close()
    conn.close()
    print("\nScoring complete.")


if __name__ == "__main__":
    run()
