# TrafficScore Ottawa

A web app that scores Ottawa road segments by traffic quality to help home buyers evaluate neighbourhoods. Scores are 0–100, where **higher = more traffic stress**.

## What It Does

- Colour-codes every named road in Ottawa on an interactive map (green → red)
- Search by street name or full address to see a score breakdown
- Shows a neighbourhood average score for any location
- Score components: traffic volume, speed, collision safety, and cut-through risk

## Score Components

| Component   | Weight | Source                                       |
|-------------|--------|----------------------------------------------|
| Volume      | 45%    | TomTom probe count (percentile rank)         |
| Speed       | 15%    | Absolute p85 speed in km/h                   |
| Safety      | 20%    | Collision density/km (City of Ottawa open data) |
| Cut-through | 20%    | OSM network graph analysis (residential only) |

## Score Categories

| Range | Label       | Colour |
|-------|-------------|--------|
| 0–20  | Very quiet  | Green  |
| 20–35 | Quiet       | Yellow |
| 35–65 | Moderate    | Orange |
| 65+   | Busy        | Red    |

## Stack

- **Database**: PostgreSQL 15 + PostGIS 3.3 (Docker)
- **Backend**: FastAPI (`api/main.py`)
- **Frontend**: Leaflet.js (`frontend/index.html`) — single HTML file, no build step
- **Scoring**: Python scripts in `scoring/`
- **Data ingestion**: Python scripts in `ingestion/`

## Running Locally

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- TomTom Traffic Stats data (requires TomTom API access — not included)
- City of Ottawa collision dataset ([open.ottawa.ca](https://open.ottawa.ca))

### Setup

```bash
# 1. Clone and install dependencies
git clone https://github.com/YOUR_USERNAME/trafficscore.git
cd trafficscore
pip install -r requirements.txt

# 2. Start the database
docker compose up -d db

# 3. Set environment variable
echo "DATABASE_URL=postgresql://trafficscore:trafficscore@localhost:5432/trafficscore" > .env
```

### Ingest Data (run once, or to refresh)

```bash
python -m ingestion.osm_ingest        # OSM road network (required first)
python -m ingestion.tomtom_ingest     # TomTom traffic data
python -m ingestion.ottawa_collisions # Ottawa collision open data
```

### Run Scoring Pipeline

```bash
python -m scoring.cutthrough   # Cut-through risk scores
python -m scoring.scorer       # Full composite scoring
```

### Start the App

```bash
uvicorn api.main:app --reload
```

Open [http://localhost:8000](http://localhost:8000).

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/segments` | GeoJSON of scored segments (bbox-filtered, up to 50k) |
| GET | `/api/segments/{id}` | Score breakdown for one segment |
| GET | `/api/segments/nearby` | Segments near a lat/lng point |
| GET | `/api/search?name=` | Street name search with abbreviation expansion |
| GET | `/api/geocode?q=` | Forward geocoding proxy (Nominatim) |
| GET | `/api/reverse-geocode?lat=&lng=` | Reverse geocoding proxy — returns suburb name |
| GET | `/api/neighbourhood?lat=&lng=&radius_m=` | Avg residential score within radius (default 600m) |

## Project Structure

```
trafficscore/
├── api/
│   └── main.py               FastAPI backend
├── db/
│   └── schema.sql            Database schema
├── frontend/
│   └── index.html            Leaflet map (no build step)
├── ingestion/
│   ├── osm_ingest.py         Fetch Ottawa road network from OSM
│   ├── tomtom_ingest.py      Load TomTom CSV traffic data
│   ├── ottawa_collisions.py  Load collision open data
│   ├── ottawa_volumes.py     Load Ottawa intersection volume counts
│   └── here_poller.py        HERE Traffic Flow poller (experimental)
├── scoring/
│   ├── scorer.py             Full scoring pipeline
│   └── cutthrough.py         Cut-through risk analysis
├── docker-compose.yml
└── requirements.txt
```

## Known Limitations

- Ottawa-specific: OSM extract and collision data are scoped to Ottawa
- TomTom data is not included (commercial licence required)
- ~261 segments with no TomTom coverage (mostly rural Ottawa) display grey on the map
- Cut-through score is only computed for residential/unclassified streets; arterials show N/A
- Hosted at [trafficscore.myke.org](https://trafficscore.myke.org) (Ottawa only)

## Data Sources

| Source | Coverage | Notes |
|--------|----------|-------|
| TomTom Traffic Stats | August 2024 | Trial account — one month snapshot. A paid account supports up to 732 days of history, which would smooth out seasonal variation and give more reliable averages. |
| City of Ottawa — Collision Data | 2017–2024 | Used for safety score component |
| City of Ottawa — Intersection Volumes | 2018–2023 | Used for trend score (currently stored but not weighted in composite) |
| OpenStreetMap | Current at time of ingest | Road network via osmnx |
