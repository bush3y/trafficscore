# TrafficScore Ottawa — Project Notes

## What It Is
A web app that scores Ottawa streets by traffic quality (0–100, higher = worse) to help home buyers evaluate neighbourhoods. Scores road segments from OSM using TomTom traffic data, collision records, and network analysis.

## Deployment
- **Live at**: https://trafficscore.myke.org
- **Server**: DigitalOcean droplet (146.190.62.0), user `mbushey`
- **Repo files**: `~/docker/trafficscore/`
- **Stack on server**: nginx → Docker (port 8383) → FastAPI + PostGIS
- **CI/CD**: GitHub Actions deploys on push to `master` via `appleboy/ssh-action`
- **SSL**: Cloudflare Flexible (handles HTTPS, nginx serves plain HTTP)
- **DNS**: `trafficscore.myke.org` A record in Cloudflare (myke.org zone)
- **Deploy key**: `~/.ssh/pucklink_deploy` (same droplet as PuckLink)

## Stack
- **DB**: PostgreSQL 15 + PostGIS 3.3 (Docker)
- **Backend**: FastAPI (`api/main.py`) — serves GeoJSON + proxies Nominatim geocoding
- **Frontend**: Leaflet.js (`frontend/index.html`) — single HTML file, no build step
- **Scoring**: Python scripts in `scoring/` — run manually to recompute
- **Ingestion**: Python scripts in `ingestion/` — run once (or on refresh)

## Running Locally
```bash
docker compose up -d db
uvicorn api.main:app --reload   # API at localhost:8000
```

## Data Currency
| Source | Coverage | Notes |
|--------|----------|-------|
| TomTom Traffic Stats | August 2024 | Trial account — one month. Paid plan supports 732 days. |
| City of Ottawa — Collisions | 2017–2024 | Safety score component |
| City of Ottawa — Intersection Volumes | 2018–2023 | Trend score (stored, not weighted in composite) |
| OpenStreetMap | Current at ingest time | Road network via osmnx |

## Score Components & Weights
| Component    | Weight | Notes |
|-------------|--------|-------|
| Volume      | 45%    | TomTom probe count (PERCENT_RANK within all segments) |
| Speed       | 15%    | Absolute p85 speed in km/h (not ratio — avoids penalizing 30 km/h zones) |
| Safety      | 20%    | Collision density/km, ≥3 collisions to count, volume-discounted (see below) |
| Cutthrough  | 20%    | Network graph analysis — only applies to residential/unclassified streets (NULL for arterials) |

**Speed is absolute, not ratio**: a 30 km/h zone going 33 km/h was scoring 84th percentile under ratio approach — deliberate decision to use absolute.

**Volume is NOT normalized by road class**: arterials score higher than residentials globally. This is intentional — a home buyer should see absolute quietness, not quietness relative to road type. The "Compared to similar streets" card section handles the relative comparison separately.

**Safety volume-discount**: Safety contribution scaled by `LEAST(1.0, volume_score / 60.0)` — full weight only kicks in at the 60th percentile for volume. Prevents isolated fender-benders on quiet residential streets from dominating. Threshold is ≥3 collisions to filter out single minor incidents.

## Score Categories (frontend)
- **0–20**: Very quiet (green)
- **20–35**: Quiet (yellow)
- **35–65**: Moderate (orange)
- **65+**: Busy / concerning (red)

## TomTom Matching Strategy (scorer.py Step 2)
1. **Direct match**: TomTom segments assigned to OSM segment in Step 1 (nearest-neighbour). Requires `probe_count >= 50` to filter noise. **Residential/unclassified/living_street cap: `probe_count > 200` excluded** — prevents arterial traffic backed up at intersections from contaminating adjacent residential streets.
2. **Spatial fallback**: ST_DWithin(0.001°, ~80m) for OSM segments that got no direct matches. Same residential probe cap applied.
3. **Short segment fix**: Segments <60m (intersection approaches) inherit TomTom data from adjacent same-name segments ≥60m — prevents near-stopped intersection approach speeds distorting scores.
4. **Name-aware matching**: `road_names_match()` plpgsql function does word-by-word prefix comparison. Prevents cross-road TomTom data contamination (e.g. Queensway bleeding onto adjacent tertiary roads).

## Key Calibration Streets
- **Churchill Ave North**: Red/busy (secondary arterial) — scoring 80–85 ✓
- **Dovercourt Avenue**: Moderate-busy (tertiary, has bus route) — scoring 42–65 ✓
- **Eye Bright Crescent**: Quiet residential, low volume — scoring 25–29 ✓
- **North Bluff Drive**: Tertiary collector, moderate — scoring 45–71 ✓
- **Evered Avenue**: Residential, 30 km/h zone — scoring 34–46 (3 collisions at threshold)
- **Canyon Walk Drive**: Wide spread (47–79) — top segment has 19 collisions near it, legitimate

## Collision Search Radius
`0.0003°` (~33m) — tightened from 0.0005° to reduce intersection spillover into adjacent residential segments.

## Known Issues / Design Decisions
- Cutthrough score is NULL (not 0) for arterials so the composite formula excludes that component entirely for non-residential roads
- **Crescent/loop fix**: Cutthrough scoring checks whether both endpoints connect to the *same* arterial road name — if yes, it's a crescent and scores 0.1 instead of high risk.
- **JavaScript ID precision**: segment IDs are 56-bit integers, exceed JS float precision (2^53). All frontend fetches that need a segment by ID must use geometry passed from the feature click — never fetch `/api/segments/{id}` from JS using props.id. API uses `BETWEEN id-32 AND id+32` pattern for any SQL ID lookups from JS-provided values.
- Trend score only covers ~2,454 segments (intersection volume data is sparse) — excluded from composite, stored but not weighted in.
- **No-data segments** (261 total, mostly rural Ottawa outside TomTom coverage): score 0 but displayed grey. Do not confuse with genuinely quiet streets.
- **Near-zero volume segments** (e.g. Hampton Avenue): TomTom had almost no probe data — score is near 0 but reflects data sparsity, not genuine quietness. No fix without more TomTom coverage.
- **Unnamed OSM segments** (1,122 on residential/tertiary/secondary roads): for address search, closest segment is always used and if unnamed, street name is inferred from search query.
- **Nearby comparison for arterials**: secondary/primary roads often have no other same-class streets within 600m — nearby line correctly hidden. City-wide comparison still shows.

## API Endpoints
- `GET /api/segments` — GeoJSON of scored segments (bbox-filtered, up to 50k)
- `GET /api/segments/{id}` — detail for one segment
- `GET /api/segments/nearby` — segments near lat/lng, sorted by distance
- `GET /api/search?name=` — street name search with abbreviation expansion (ave→avenue, n→north, etc.)
- `GET /api/geocode?q=` — proxy to Nominatim forward geocoding
- `GET /api/reverse-geocode?lat=&lng=` — proxy to Nominatim reverse (returns suburb name)
- `GET /api/neighbourhood?lat=&lng=&radius_m=` — avg composite score of residential streets within radius (default 600m)
- `GET /api/relative?road_class=&score=&lat=&lng=&radius_m=` — percentile rank within road class, city-wide and nearby (distinct street names, min 3 to show nearby)

## Frontend Features
- **Map**: Leaflet, colour-coded segments (green/yellow/orange/red), grey for no-data. Zoom buttons hidden on mobile.
- **Street search**: searches by name with abbreviation expansion; opens sidebar with score breakdown
- **Address search**: geocodes via Nominatim, shows closest segment score card + neighbourhood avg; infers street name for unnamed segments from query
- **Sidebar**: score breakdown + async neighbourhood score + "Compared to similar streets" (city-wide and nearby percentile within road class)
- **Mobile bottom sheet**: peek state (185px, shows name + score) → tap handle to expand → tap again to collapse. Sizes to content height, max calc(100vh - 120px).
- **Mobile controls**: filters (road type + score slider) collapsed behind "Filters ▾" toggle by default
- **Nearby streets**: collapsible section in address search results, collapsed by default on mobile
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
- **TomTom refresh**: trial was August 2024 only. Paid plan would allow monthly/quarterly refresh. HERE Traffic Analytics is an alternative but requires new ingestion script + segment matching logic.
- Re-evaluate trend score weight (currently stored but not used in composite)
- Broader calibration pass across more streets/neighbourhoods

## Ingestion Scripts
```bash
python -m ingestion.osm_ingest          # Fetch OSM road network
python -m ingestion.tomtom_ingest       # Load TomTom data (default: Aug 2024 trial range)
python -m ingestion.ottawa_collisions   # Load collision data
python -m scoring.cutthrough            # Compute cut-through risk scores
python -m scoring.scorer                # Run full scoring pipeline
```
