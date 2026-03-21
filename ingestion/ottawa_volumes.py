"""
Ottawa intersection volume ingestion.

Loads annual intersection volume CSVs downloaded from Ottawa Open Data.
Download the CSV files for each year from open.ottawa.ca (search "Intersection Volume")
and place them in a local data/ directory.

Usage:
    python -m ingestion.ottawa_volumes --dir ./data/volumes
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

# Ottawa Open Data column name variations across years
# We normalize these into a common schema
COLUMN_MAPS = [
    # 2022, 2023 format
    {
        "name": ["LOCATION", "INTERSECTION", "intersection_name", "Name"],
        "volume": ["VOLUME", "volume", "Total_Volume", "COUNT"],
        "lat": ["Y", "LAT", "LATITUDE", "lat"],
        "lon": ["X", "LON", "LONGITUDE", "lon"],
        "year": None,  # derived from filename
    },
]


def normalize_columns(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Normalize column names across different Ottawa Open Data CSV formats."""
    cols = {c.upper(): c for c in df.columns}

    def find_col(candidates):
        for c in candidates:
            if c.upper() in cols:
                return cols[c.upper()]
        return None

    name_col = find_col(["LOCATION", "INTERSECTION", "INTERSECTION_NAME", "NAME", "STREET"])
    vol_col = find_col(["VOLUME", "TOTAL_VOLUME", "COUNT", "ADT", "AADT"])
    lat_col = find_col(["Y", "LAT", "LATITUDE"])
    lon_col = find_col(["X", "LON", "LONG", "LONGITUDE"])

    if not all([name_col, vol_col, lat_col, lon_col]):
        print(f"  Warning: could not map all columns in {filename}")
        print(f"  Available columns: {list(df.columns)}")
        return None

    # Try to extract year from filename (e.g. "intersection_volume_2022.csv")
    year = None
    for part in Path(filename).stem.split("_"):
        if part.isdigit() and len(part) == 4:
            year = int(part)
            break

    result = pd.DataFrame({
        "intersection_name": df[name_col],
        "volume": pd.to_numeric(df[vol_col], errors="coerce"),
        "lat": pd.to_numeric(df[lat_col], errors="coerce"),
        "lon": pd.to_numeric(df[lon_col], errors="coerce"),
        "year": year,
    }).dropna(subset=["lat", "lon", "volume"])

    return result


def load_file(filepath: str, conn):
    filename = os.path.basename(filepath)
    print(f"Loading {filename}...")

    df = pd.read_csv(filepath)
    normalized = normalize_columns(df, filename)

    if normalized is None or normalized.empty:
        print(f"  Skipped (could not parse)")
        return 0

    rows = [
        (
            str(row.intersection_name),
            int(row.year) if row.year else None,
            int(row.volume),
            row.lon,
            row.lat,
        )
        for row in normalized.itertuples()
    ]

    cur = conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO intersection_volumes (intersection_name, year, volume, geometry)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
        template="(%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))",
    )
    conn.commit()
    cur.close()
    print(f"  Loaded {len(rows)} intersections")
    return len(rows)


def run(data_dir: str):
    path = Path(data_dir)
    csv_files = list(path.glob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        print("Download intersection volume CSVs from open.ottawa.ca and place them here.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    total = 0
    try:
        for f in sorted(csv_files):
            total += load_file(str(f), conn)
    finally:
        conn.close()

    print(f"\nDone. {total} intersection volumes loaded.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Ottawa intersection volume CSVs")
    parser.add_argument("--dir", default="./data/volumes", help="Directory containing CSV files")
    args = parser.parse_args()
    run(args.dir)
