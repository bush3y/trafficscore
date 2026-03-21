CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================
-- Road network base layer (from OpenStreetMap via Overpass API)
-- ============================================================
CREATE TABLE IF NOT EXISTS road_segments (
    id              BIGINT PRIMARY KEY,  -- OSM way ID
    name            TEXT,
    road_class      TEXT,                -- OSM highway tag value
    speed_limit     INTEGER,             -- km/h (from OSM maxspeed tag)
    lanes           INTEGER,
    oneway          BOOLEAN DEFAULT FALSE,
    cutthrough_risk FLOAT,               -- computed 0.0-1.0, null until scored
    geometry        GEOMETRY(LineString, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS road_segments_geom_idx ON road_segments USING GIST(geometry);
CREATE INDEX IF NOT EXISTS road_segments_class_idx ON road_segments(road_class);

-- ============================================================
-- TomTom Traffic Stats segments (one-time trial pull)
-- Probe count = relative volume proxy (more probes = more vehicles)
-- Speed percentiles indicate how fast traffic actually moves
-- ============================================================
CREATE TABLE IF NOT EXISTS tomtom_segments (
    id              BIGSERIAL PRIMARY KEY,
    tomtom_id       TEXT NOT NULL,
    frc             INTEGER,             -- Functional Road Class: 0=highway, 7=residential
    road_name       TEXT,
    probe_count     INTEGER,             -- relative volume (GPS probes observed)
    avg_speed_kmh   FLOAT,
    speed_p85_kmh   FLOAT,               -- 85th percentile speed
    speed_p95_kmh   FLOAT,               -- 95th percentile speed
    time_set        TEXT,                -- all_day | am_peak | pm_peak
    date_from       DATE,
    date_to         DATE,
    pulled_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    segment_id      BIGINT REFERENCES road_segments(id),  -- spatial match to OSM, nullable
    geometry        GEOMETRY(LineString, 4326)
);
CREATE UNIQUE INDEX IF NOT EXISTS tomtom_segments_unique_idx
    ON tomtom_segments(tomtom_id, time_set, pulled_at);
CREATE INDEX IF NOT EXISTS tomtom_segments_geom_idx ON tomtom_segments USING GIST(geometry);
CREATE INDEX IF NOT EXISTS tomtom_segments_frc_idx ON tomtom_segments(frc);

-- ============================================================
-- HERE Traffic Flow observations (polled every 2 hours, ongoing)
-- Builds our own historical speed/congestion dataset over time
-- ============================================================
CREATE TABLE IF NOT EXISTS here_flow_observations (
    id              BIGSERIAL PRIMARY KEY,
    here_link_id    TEXT NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL,
    speed_ms        FLOAT,               -- current speed in m/s
    free_flow_ms    FLOAT,               -- free flow speed in m/s
    jam_factor      FLOAT,               -- HERE congestion score 0.0-10.0
    confidence      FLOAT,               -- 0.0-1.0 (>0.7 = real-time data)
    traversability  TEXT,
    segment_id      BIGINT REFERENCES road_segments(id),  -- spatial match, nullable
    geometry        GEOMETRY(LineString, 4326)
);
CREATE INDEX IF NOT EXISTS here_flow_link_time_idx
    ON here_flow_observations(here_link_id, observed_at);
CREATE INDEX IF NOT EXISTS here_flow_obs_time_idx ON here_flow_observations(observed_at);
CREATE INDEX IF NOT EXISTS here_flow_geom_idx ON here_flow_observations USING GIST(geometry);

-- ============================================================
-- Ottawa intersection volume counts (Ottawa Open Data, 2018-2023)
-- Annual snapshots — used for multi-year trend analysis
-- ============================================================
CREATE TABLE IF NOT EXISTS intersection_volumes (
    id                  SERIAL PRIMARY KEY,
    intersection_name   TEXT,
    year                INTEGER NOT NULL,
    volume              INTEGER,         -- vehicles per day at intersection
    geometry            GEOMETRY(Point, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS intersection_volumes_geom_idx
    ON intersection_volumes USING GIST(geometry);
CREATE INDEX IF NOT EXISTS intersection_volumes_year_idx ON intersection_volumes(year);

-- ============================================================
-- Ottawa collision data (Ottawa Open Data, 2017-2024)
-- ============================================================
CREATE TABLE IF NOT EXISTS collisions (
    id              SERIAL PRIMARY KEY,
    collision_date  DATE,
    year            INTEGER,
    severity        TEXT,                -- property_damage | injury | fatal
    collision_type  TEXT,                -- pedestrian | cyclist | vehicle
    geometry        GEOMETRY(Point, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS collisions_geom_idx ON collisions USING GIST(geometry);
CREATE INDEX IF NOT EXISTS collisions_year_idx ON collisions(year);

-- ============================================================
-- Computed street scores (cached, refreshed on schedule)
-- Lower composite_score = more desirable (quieter, safer, stable)
-- ============================================================
CREATE TABLE IF NOT EXISTS street_scores (
    id               SERIAL PRIMARY KEY,
    segment_id       BIGINT REFERENCES road_segments(id) UNIQUE,
    volume_score     FLOAT,              -- 0-100, based on TomTom probe count
    speed_score      FLOAT,              -- 0-100, based on p85 vs speed limit
    safety_score     FLOAT,              -- 0-100, based on collision density
    cutthrough_score FLOAT,              -- 0-100, OSM connectivity analysis
    trend_score      FLOAT,              -- negative=quieter over time, positive=busier
    composite_score  FLOAT,              -- weighted overall (lower = better)
    computed_at      TIMESTAMPTZ DEFAULT NOW()
);
