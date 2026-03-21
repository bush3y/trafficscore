"""
Ottawa collision data ingestion.

Loads collision CSVs downloaded from Ottawa Open Data (2017-2024).
Download from open.ottawa.ca (search "Traffic Collision Data") and
place CSV files in a local data/ directory.

Usage:
    python -m ingestion.ottawa_collisions --dir ./data/collisions
"""

import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

# Severity mapping from Ottawa's collision classification
SEVERITY_MAP = {
    "fatal": "fatal",
    "injury": "injury",
    "non-injury": "property_damage",
    "property damage only": "property_damage",
    "pdo": "property_damage",
}

# Collision type mapping
TYPE_MAP = {
    "pedestrian": "pedestrian",
    "cyclist": "cyclist",
    "bicycle": "cyclist",
    "vehicle": "vehicle",
}


def classify_severity(val: str) -> str:
    if not val:
        return "property_damage"
    return SEVERITY_MAP.get(str(val).lower().strip(), "property_damage")


def classify_type(val: str) -> str:
    if not val:
        return "vehicle"
    v = str(val).lower()
    for key, mapped in TYPE_MAP.items():
        if key in v:
            return mapped
    return "vehicle"


def load_file(filepath: str, conn):
    filename = os.path.basename(filepath)
    print(f"Loading {filename}...")

    df = pd.read_csv(filepath, low_memory=False)
    cols_upper = {c.upper(): c for c in df.columns}

    def find(candidates):
        for c in candidates:
            if c.upper() in cols_upper:
                return cols_upper[c.upper()]
        return None

    date_col = find(["COLLISION_DATE", "DATE", "ACCIDENT_DATE", "CRASH_DATE"])
    lat_col = find(["Y", "LAT", "LATITUDE"])
    lon_col = find(["X", "LON", "LONGITUDE", "LONG"])
    severity_col = find(["COLLISION_CLASSIFICATION", "SEVERITY", "COLLISION_TYPE", "ACCIDENT_TYPE"])
    type_col = find(["INVOLVED_FACTOR", "TYPE", "COLLISION_INVOLVEMENT"])

    if not lat_col or not lon_col:
        print(f"  Skipped — cannot find coordinates in {filename}")
        print(f"  Columns: {list(df.columns)}")
        return 0

    # Try to extract year from filename
    year = None
    for part in Path(filename).stem.split("_"):
        if part.isdigit() and len(part) == 4:
            year = int(part)
            break

    rows = []
    for _, row in df.iterrows():
        try:
            lat = float(row[lat_col]) if lat_col else None
            lon = float(row[lon_col]) if lon_col else None
            if not lat or not lon or lat == 0 or lon == 0:
                continue

            collision_date = None
            if date_col and pd.notna(row[date_col]):
                try:
                    collision_date = pd.to_datetime(row[date_col]).date()
                except Exception:
                    pass

            row_year = year or (collision_date.year if collision_date else None)
            severity = classify_severity(row[severity_col] if severity_col else None)
            col_type = classify_type(row[type_col] if type_col else None)

            rows.append((collision_date, row_year, severity, col_type, lon, lat))
        except Exception:
            continue

    if not rows:
        print(f"  No valid rows found in {filename}")
        return 0

    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO collisions (collision_date, year, severity, collision_type, geometry)
        VALUES %s
        """,
        rows,
        template="(%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))",
    )
    conn.commit()
    cur.close()
    print(f"  Loaded {len(rows)} collisions")
    return len(rows)


def run(data_dir: str):
    path = Path(data_dir)
    csv_files = list(path.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        print("Download collision CSVs from open.ottawa.ca and place them here.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    total = 0
    try:
        for f in sorted(csv_files):
            total += load_file(str(f), conn)
    finally:
        conn.close()

    print(f"\nDone. {total} collisions loaded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Ottawa collision CSVs")
    parser.add_argument("--dir", default="./data/collisions", help="Directory containing CSV files")
    args = parser.parse_args()
    run(args.dir)
