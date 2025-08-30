#!/usr/bin/env python3
"""
Load a CSV that contains a WKT geometry column (e.g., 'the_geom') into MongoDB as GeoJSON.

Features:
- Parses WKT (POINT/LINESTRING/POLYGON/MULTIPOLYGON, etc.) to GeoJSON
- Optional CRS transform (e.g., EPSG:2263 -> EPSG:4326) for Mongo 2dsphere
- Streams CSV (supports .csv or .csv.gz; handles quoted multiline WKT)
- Converts empty strings to None; tries int/float coercion for non-geom columns
- Batching + optional drop + 2dsphere index creation
- Reads MONGO_URI from .env (python-dotenv), with CLI override if desired

Usage:
  python load_wkt_csv_to_mongo.py \
    --csv flood_vulnerability.csv \
    --db geo \
    --collection fvi \
    --wkt-field the_geom \
    --crs-in EPSG:4326 \
    --crs-out EPSG:4326 \
    --drop \
    --create-index

If your WKT is in a local/projection CRS (e.g., EPSG:2263 for NYC),
set --crs-in accordingly and leave --crs-out as EPSG:4326.
"""

import argparse
import csv
import gzip
import io
import logging
import os
from typing import Any, Dict, Iterable, List, Optional

from pymongo import InsertOne, MongoClient
from pymongo.errors import BulkWriteError

# Optional .env
try:
    from dotenv import load_dotenv

    _HAS_DOTENV = True
except Exception:
    _HAS_DOTENV = False

from pyproj import CRS, Transformer

# Geometry / CRS
from shapely import wkt
from shapely.errors import WKTReadingError
from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry


def open_maybe_gzip(path: str, encoding: str = "utf-8") -> io.TextIOBase:
    if path.lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding=encoding, newline="")
    return open(path, "r", encoding=encoding, newline="")


def coerce_value(v: Optional[str]) -> Any:
    if v is None:
        return None
    v = v.strip()
    if v == "" or v.upper() in {"NULL", "N/A", "NA", "NONE"}:
        return None
    # Try int
    if v.isdigit() and not (len(v) > 1 and v[0] == "0"):
        try:
            return int(v)
        except Exception:
            pass
    # Try float (remove thousands commas)
    try:
        return float(v.replace(",", ""))
    except Exception:
        return v


def parse_wkt(geom_wkt: str) -> BaseGeometry:
    try:
        geom = wkt.loads(geom_wkt)
        # Attempt simple validity fix (self-intersections)
        if not geom.is_valid:
            geom = geom.buffer(0)
        return geom
    except WKTReadingError as e:
        raise ValueError(f"Invalid WKT geometry: {e}") from e


def geom_to_geojson(geom: BaseGeometry) -> Dict[str, Any]:
    """Convert shapely geometry to a GeoJSON-like dict."""
    return mapping(geom)


def reproject_geometry(geom: BaseGeometry, src_crs: str, dst_crs: str) -> BaseGeometry:
    if src_crs == dst_crs:
        return geom
    transformer = Transformer.from_crs(
        CRS.from_user_input(src_crs), CRS.from_user_input(dst_crs), always_xy=True
    )
    # Apply coordinate transform
    return shapely_transform_coords(geom, transformer)


def shapely_transform_coords(
    geom: BaseGeometry, transformer: Transformer
) -> BaseGeometry:
    # avoid importing shapely.ops.transform directly to keep deps lean
    from shapely.ops import transform as ops_transform

    return ops_transform(lambda x, y, z=None: transformer.transform(x, y), geom)


def batch_iter(
    iterable: Iterable[Dict[str, Any]], n: int
) -> Iterable[List[Dict[str, Any]]]:
    buf: List[Dict[str, Any]] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def yield_docs_from_csv(
    csv_path: str,
    wkt_field: str,
    geometry_field: str,
    crs_in: str,
    crs_out: str,
    encoding: str = "utf-8",
) -> Iterable[Dict[str, Any]]:
    with open_maybe_gzip(csv_path, encoding=encoding) as f:
        # csv module handles quoted fields with embedded newlines properly
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV appears to have no header row.")
        if wkt_field not in reader.fieldnames:
            raise ValueError(f"WKT field '{wkt_field}' not found in CSV headers.")

        for row in reader:
            raw_wkt = row.get(wkt_field)
            if raw_wkt is None or raw_wkt.strip() == "":
                # Skip rows without geometry
                continue

            # Parse and reproject geometry
            geom = parse_wkt(raw_wkt)
            geom = reproject_geometry(geom, crs_in, crs_out)
            geom_geojson = geom_to_geojson(geom)

            # Build document: geometry + all other fields coerced
            doc: Dict[str, Any] = {geometry_field: geom_geojson}
            for k, v in row.items():
                if k == wkt_field:
                    continue
                doc[k] = coerce_value(v)
            yield doc


def main():
    if _HAS_DOTENV:
        load_dotenv()

    parser = argparse.ArgumentParser(
        description="Load WKT geometries from CSV into MongoDB as GeoJSON."
    )
    parser.add_argument("--csv", required=True, help="Path to CSV (.csv or .csv.gz)")
    parser.add_argument("--db", required=True, help="MongoDB database name")
    parser.add_argument("--collection", required=True, help="MongoDB collection name")
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB URI (env MONGO_URI is respected)",
    )
    parser.add_argument(
        "--wkt-field",
        default="the_geom",
        help="CSV column containing WKT (default: the_geom)",
    )
    parser.add_argument(
        "--geometry-field",
        default="geometry",
        help="Field name to store GeoJSON (default: geometry)",
    )
    parser.add_argument(
        "--crs-in", default="EPSG:4326", help="Source CRS of WKT (e.g., EPSG:2263)"
    )
    parser.add_argument(
        "--crs-out",
        default="EPSG:4326",
        help="Target CRS for Mongo (should be EPSG:4326)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Bulk insert batch size (default: 2000)",
    )
    parser.add_argument(
        "--create-index",
        action="store_true",
        help="Create 2dsphere index on geometry field",
    )
    parser.add_argument(
        "--drop", action="store_true", help="Drop the collection before loading"
    )
    parser.add_argument(
        "--encoding", default="utf-8", help="CSV encoding (default: utf-8)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Connect
    client = MongoClient(args.mongo_uri)
    db = client[args.db]
    coll = db[args.collection]

    # Optionally drop
    if args.drop:
        logging.info("Dropping %s.%s", args.db, args.collection)
        coll.drop()

    # Optional index
    if args.create_index:
        logging.info("Ensuring 2dsphere index on '%s'", args.geometry_field)
        coll.create_index([(args.geometry_field, "2dsphere")])

    total = 0
    logging.info(
        "Loading %s (WKT field: %s, CRS %s -> %s) into %s.%s",
        args.csv,
        args.wkt_field,
        args.crs_in,
        args.crs_out,
        args.db,
        args.collection,
    )

    try:
        for i, docs in enumerate(
            batch_iter(
                yield_docs_from_csv(
                    args.csv,
                    wkt_field=args.wkt_field,
                    geometry_field=args.geometry_field,
                    crs_in=args.crs_in,
                    crs_out=args.crs_out,
                    encoding=args.encoding,
                ),
                args.batch_size,
            ),
            start=1,
        ):
            res = coll.bulk_write([InsertOne(d) for d in docs], ordered=False)
            total += res.inserted_count
            logging.info(
                "Batch %d inserted %d (total %d)", i, res.inserted_count, total
            )
    except BulkWriteError as bwe:
        logging.error("Bulk write error: %s", bwe.details)
        raise
    except Exception as e:
        logging.exception("Failed to load CSV: %s", e)
        raise

    logging.info("Done. Inserted %d documents.", total)
    logging.info("Reminder: MongoDB geospatial expects GeoJSON in WGS84 (EPSG:4326).")


if __name__ == "__main__":
    main()
