# TrafficScore Ottawa — Project Notes

## What It Is
A web app that scores Ottawa streets by traffic quality (0–100, higher = worse) to help home buyers evaluate neighbourhoods. Scores road segments from OSM using TomTom traffic data, collision records, and network analysis.

## Stack
- **DB**: PostgreSQL + PostGIS (Docker)
- **Backend**: FastAPI (`api/main.py`) — serves GeoJSON + proxies Nominatim geocoding
- **Frontend**: Leaflet.js (`frontend/index.html`) — single HTML file, no build step
- **Scoring**: Python scripts in `scoring/` — run manually to recompute
- **Ingestion**: Python scripts in `ingestion/` — run once (or on refresh)

## Running Locally
```bash
docker compose up -d db
uvicorn api.main:app --reload   # API at localhost:8000
```

## Score Components & Weights
| Component    | Weight | Notes |
|-------------|--------|-------|
| Volume      | 45%    | TomTom probe count (PERCENT_RANK within all segments) |
| Speed       | 15%    | Absolute p85 speed in km/h (not ratio — avoids penalizing 30 km/h zones) |
| Safety      | 20%    | Collision density/km, ≥3 collisions to count, volume-discounted (see below) |
| Cutthrough  | 20%    | Network graph analysis — only applies to residential/unclassified streets (NULL for arterials) |

**Safety volume-discount**: Safety contribution scaled by `LEAST(1.0, volume_score / 60.0)` — full weight only kicks in at the 60th percentile for volume. Streets below that get progressively discounted. Prevents isolated fender-benders on quiet residential streets from dominating the composite. Threshold is ≥3 collisions (not ≥2) to filter out single minor incidents.

## Score Categories (frontend)
- **0–20**: Very quiet (green)
- **20–35**: Quiet (yellow)
- **35–65**: Moderate (orange)
- **65+**: Busy / concerning (red)

## TomTom Matching Strategy (scorer.py Step 2)
1. **Direct match**: TomTom segments assigned to OSM segment in Step 1 (nearest-neighbour). Requires `probe_count >= 50` to filter noise. **Residential/unclassified/living_street cap: `probe_count > 200` excluded** — prevents arterial traffic backed up at intersections (going slowly, high probe count) from contaminating adjacent residential streets.
2. **Spatial fallback**: ST_DWithin(0.001°, ~80m) for OSM segments that got no direct matches. Same residential probe cap applied.
3. **Short segment fix**: Segments <60m (intersection approaches) inherit TomTom data from adjacent same-name segments ≥60m — prevents near-stopped intersection approach speeds distorting scores.
4. **Name-aware matching**: `road_names_match()` plpgsql function does word-by-word prefix comparison. Used in Step 1 ordering and Step 2 to prevent cross-road TomTom data contamination (e.g. Queensway data bleeding onto adjacent tertiary roads, Island Park Dr data contaminating Island Park Crescent).

## Key Calibration Streets
- **Churchill Ave North**: Red/busy (secondary arterial) — scoring 80–85 ✓
- **Dovercourt Avenue**: Moderate-busy (tertiary, has bus route) — scoring 42–65 ✓
- **Eye Bright Crescent**: Quiet residential, low volume — scoring 25–29 ✓
- **North Bluff Drive**: Tertiary collector, moderate — scoring 45–71 ✓
- **Evered Avenue**: Residential, 30 km/h zone — scoring 34–46 (3 collisions at threshold)
- **Canyon Walk Drive**: Wide spread (47–79) — top segment has 19 collisions near it, legitimate

## Collision Search Radius
`0.0003°` (~33m) — tightened from 0.0005° to reduce intersection spillover into adjacent residential segments.

## Safety Score Threshold
`CASE WHEN num_collisions >= 3 THEN num_collisions ELSE 0 END` — fewer than 3 collisions don't count. Reduces noise on low-traffic streets.

## Known Issues / Design Decisions
- Speed metric is **absolute p85** (not ratio vs speed limit) — a 30 km/h zone going 33 km/h was scoring 84th percentile under ratio approach
- Cutthrough score is NULL (not 0) for arterials so the composite formula excludes that component entirely for non-residential roads
- **Crescent/loop fix**: Cutthrough scoring checks whether both endpoints connect to the *same* arterial road name — if yes, it's a crescent (no shortcut value) and scores 0.1 instead of high risk.
- JavaScript ID precision: segment IDs are 56-bit integers, exceed JS float precision (2^53). Frontend should avoid exact equality; API uses `BETWEEN id-32 AND id+32` pattern if needed.
- Trend score only covers ~2,454 segments (intersection volume data is sparse) — excluded from composite, stored but not weighted in.
- **No-data segments** (261 total, mostly rural Ottawa outside TomTom coverage): score 0 but are displayed grey on the map. Do not confuse with genuinely quiet streets.
- **Unnamed OSM segments** (1,122 on residential/tertiary/secondary roads): exist because mappers added geometry without name tags. For address search, the closest segment is always used (correct score) and if unnamed, the street name is inferred from the search query.

## API Endpoints
- `GET /api/segments` — GeoJSON of scored segments (bbox-filtered, up to 50k)
- `GET /api/segments/{id}` — detail for one segment
- `GET /api/segments/nearby` — segments near lat/lng, sorted by distance
- `GET /api/search?name=` — street name search with abbreviation expansion (ave→avenue, n→north, etc.)
- `GET /api/geocode?q=` — proxy to Nominatim forward geocoding
- `GET /api/reverse-geocode?lat=&lng=` — proxy to Nominatim reverse (returns suburb name)
- `GET /api/neighbourhood?lat=&lng=&radius_m=` — avg composite score of residential streets within radius (default 600m)

## Frontend Features
- **Map**: Leaflet, colour-coded segments (green/yellow/orange/red), grey for no-data
- **Street search**: searches by name with abbreviation expansion; opens sidebar with score breakdown
- **Address search**: geocodes via Nominatim, shows closest segment score card + neighbourhood avg; infers street name for unnamed segments from query
- **Sidebar**: score breakdown (volume/speed/safety/cutthrough bars) + async neighbourhood score with suburb name
- **Mobile**: responsive — controls stretch full-width, sidebar becomes bottom sheet at ≤600px
- **Geocode cache**: repeat address searches skip Nominatim round-trip (client-side Map)
- **AbortController**: cancels in-flight requests on new search

## Score Distribution (last checked)
| Band | Segments | % |
|------|----------|---|
| 0–20 Very quiet | 7,194 | 22% |
| 20–35 Quiet | 8,480 | 26% |
| 35–65 Moderate | 8,308 | 25% |
| 65+ Busy | 8,774 | 27% |

## Pending / Future Ideas
- **Neighbourhood browse**: search by neighbourhood name ("Westboro") and get aggregate score card
- **Share/deep links**: URLs that open map at a specific street or address
- **Deployment**: currently local only; needs a server to share with real users
- Re-evaluate trend score weight (currently stored but not used in composite)
- Broader calibration pass across more streets/neighbourhoods

## Ingestion Scripts
```bash
python -m ingestion.osm_ingest          # Fetch OSM road network
python -m ingestion.tomtom_ingest       # Load TomTom CSV data
python -m ingestion.collision_ingest    # Load collision data
python -m scoring.cutthrough            # Compute cut-through risk scores
python -m scoring.scorer                # Run full scoring pipeline
```
