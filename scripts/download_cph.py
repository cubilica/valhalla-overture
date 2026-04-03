#!/usr/bin/env python3
"""Download Copenhagen transportation data from Overture Maps as Parquet."""

import os
import time

import duckdb

SOUTH, WEST, NORTH, EAST = 55.60, 12.40, 55.75, 12.70

OVERTURE_RELEASE = "2026-03-18.0"
S3_BASE = f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE}"

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
os.makedirs(OUT_DIR, exist_ok=True)

db = duckdb.connect()
db.execute("INSTALL spatial; INSTALL httpfs;")
db.execute("LOAD spatial; LOAD httpfs; SET s3_region='us-west-2';")

print("Downloading connectors...", flush=True)
t0 = time.time()
db.execute(f"""
    COPY (
        SELECT *
        FROM read_parquet('{S3_BASE}/theme=transportation/type=connector/*')
        WHERE bbox.xmin > {WEST} AND bbox.xmax < {EAST}
          AND bbox.ymin > {SOUTH} AND bbox.ymax < {NORTH}
    ) TO '{OUT_DIR}/connectors.parquet' (FORMAT PARQUET)
""")
count = db.execute(f"SELECT count(*) FROM '{OUT_DIR}/connectors.parquet'").fetchone()[0]
print(f"{count} connectors in {time.time() - t0:.1f}s", flush=True)

print("Downloading segments...", flush=True)
t1 = time.time()
db.execute(f"""
    COPY (
        SELECT *
        FROM read_parquet('{S3_BASE}/theme=transportation/type=segment/*')
        WHERE bbox.xmin > {WEST} AND bbox.xmax < {EAST}
          AND bbox.ymin > {SOUTH} AND bbox.ymax < {NORTH}
    ) TO '{OUT_DIR}/segments.parquet' (FORMAT PARQUET)
""")
count = db.execute(f"SELECT count(*) FROM '{OUT_DIR}/segments.parquet'").fetchone()[0]
print(f"{count} segments in {time.time() - t1:.1f}s", flush=True)

c_size = os.path.getsize(f"{OUT_DIR}/connectors.parquet")
s_size = os.path.getsize(f"{OUT_DIR}/segments.parquet")
print(f"connectors.parquet: {c_size / 1e6:.1f} MB")
print(f"segments.parquet: {s_size / 1e6:.1f} MB")
print(f"total time: {time.time() - t0:.0f}s")

print("\nSample segment:")
row = db.execute(f"""
    SELECT id, class, subclass, road_surface, names.primary as name,
           connectors, ST_AsText(geometry) as wkt
    FROM '{OUT_DIR}/segments.parquet'
    WHERE class = 'residential' AND names.primary IS NOT NULL
    LIMIT 1
""").fetchone()
if row:
    for i, col in enumerate(
        ["id", "class", "subclass", "road_surface", "name", "connectors", "wkt"]
    ):
        print(f"{col}: {row[i]}")
