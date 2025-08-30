#!/usr/bin/env python3
"""
Load GeoJSON into a MongoDB collection.

Features:
- Reads .geojson/.json (optionally .gz)
- Accepts Feature, FeatureCollection, [Feature, ...], or NDJSON (one JSON per line)
- Optional: move Feature.properties to root (flatten) or keep under "properties"
- Creates 2dsphere index on the geometry field (default: "geometry")
- Batching and progress logs
- Loads MONGO_URI from .env (python-dotenv) with CLI override

Usage:
  python load_geojson_to_mongo.py \
    --input data.geojson \
    --db mydb \
    --collection places \
    --drop \
    --create-index \
    --geometry-field geometry \
    --batch-size 5000

For NDJSON:
  python load_geojson_to_mongo.py --input data.ndjson --ndjson --db mydb --collection places

Env file (.env):
  MONGO_URI=mongodb://localhost:27017
"""

import argparse
import gzip
import io
import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional

from pymongo import InsertOne, MongoClient
from pymongo.errors import BulkWriteError

# Try to load dotenv if available
try:
    from dotenv import load_dotenv  # type: ignore

    _HAS_DOTENV = True
except Exception:
    _HAS_DOTENV = False


def open_maybe_gzip(path: str, encoding: str = "utf-8") -> io.TextIOBase:
    if path.lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding=encoding, newline="")
    return open(path, "r", encoding=encoding, newline="")


def is_feature(obj: Dict[str, Any]) -> bool:
    return isinstance(obj, dict) and obj.get("type") == "Feature"


def is_feature_collection(obj: Dict[str, Any]) -> bool:
    return (
        isinstance(obj, dict)
        and obj.get("type") == "FeatureCollection"
        and isinstance(obj.get("features"), list)
    )


def normalize_feature(
    feat: Dict[str, Any],
    geometry_field: str = "geometry",
    flatten_properties: bool = False,
    keep_id: bool = True,
) -> Dict[str, Any]:
    """
    Convert a GeoJSON Feature into a Mongo-ready document:
    - geometry kept under `geometry_field` (GeoJSON-compliant for 2dsphere)
    - properties either flattened into root or stored under "properties"
    - preserves Feature.id as `_feature_id` by default (to avoid clashing with Mongo's _id)
    """
    if not is_feature(feat):
        raise ValueError("Object is not a GeoJSON Feature")

    geom = feat.get("geometry")
    props = feat.get("properties", {}) or {}
    doc: Dict[str, Any] = {}

    # Geometry
    doc[geometry_field] = geom

    # Properties
    if flatten_properties and isinstance(props, dict):
        # Flatten top-level only; nested props remain nested
        doc.update(props)
    else:
        doc["properties"] = props

    # Retain feature.id if present (distinct from Mongo _id)
    if keep_id and "id" in feat:
        doc["_feature_id"] = feat["id"]

    return doc


def yield_docs_from_geojson_stream(
    f: io.TextIOBase,
    ndjson: bool,
    geometry_field: str,
    flatten_properties: bool,
    keep_id: bool,
) -> Iterable[Dict[str, Any]]:
    if ndjson:
        # one json object per line
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if is_feature(obj):
                yield normalize_feature(
                    obj, geometry_field, flatten_properties, keep_id
                )
            elif is_feature_collection(obj):
                for feat in obj["features"]:
                    yield normalize_feature(
                        feat, geometry_field, flatten_properties, keep_id
                    )
            elif isinstance(obj, list):
                for feat in obj:
                    if is_feature(feat):
                        yield normalize_feature(
                            feat, geometry_field, flatten_properties, keep_id
                        )
            else:
                raise ValueError(
                    "NDJSON line is not a Feature/FeatureCollection/array of Features"
                )
        return

    # Standard JSON / GeoJSON
    obj = json.load(f)
    if is_feature(obj):
        yield normalize_feature(obj, geometry_field, flatten_properties, keep_id)
    elif is_feature_collection(obj):
        for feat in obj["features"]:
            yield normalize_feature(feat, geometry_field, flatten_properties, keep_id)
    elif isinstance(obj, list):
        for feat in obj:
            if is_feature(feat):
                yield normalize_feature(
                    feat, geometry_field, flatten_properties, keep_id
                )
    else:
        raise ValueError(
            "Input JSON is not a Feature, FeatureCollection, or array of Features"
        )


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


def main():
    if _HAS_DOTENV:
        load_dotenv()

    parser = argparse.ArgumentParser(description="Load GeoJSON into MongoDB.")
    parser.add_argument(
        "--input", required=True, help="Path to .geojson/.json(.gz) or NDJSON"
    )
    parser.add_argument("--db", required=True, help="MongoDB database name")
    parser.add_argument("--collection", required=True, help="MongoDB collection name")
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        help="Mongo URI (env MONGO_URI is respected)",
    )
    parser.add_argument(
        "--geometry-field",
        default="geometry",
        help="Field name to store geometry (default: geometry)",
    )
    parser.add_argument(
        "--flatten-properties",
        action="store_true",
        help="Flatten Feature.properties into the root document",
    )
    parser.add_argument(
        "--keep-feature-id",
        action="store_true",
        help="Keep original Feature.id as `_feature_id`",
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
        "--batch-size", type=int, default=5000, help="Bulk insert batch size"
    )
    parser.add_argument(
        "--ndjson",
        action="store_true",
        help="Treat input as NDJSON (one JSON per line)",
    )
    parser.add_argument(
        "--encoding", default="utf-8", help="File encoding (default: utf-8)"
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

    if args.drop:
        logging.info("Dropping collection %s.%s", args.db, args.collection)
        coll.drop()

    # Optional index
    if args.create_index:
        logging.info("Ensuring 2dsphere index on '%s'", args.geometry_field)
        coll.create_index([(args.geometry_field, "2dsphere")])

    total = 0
    logging.info(
        "Loading %s into %s.%s (batch=%d, ndjson=%s)",
        args.input,
        args.db,
        args.collection,
        args.batch_size,
        args.ndjson,
    )

    with open_maybe_gzip(args.input, encoding=args.encoding) as f:
        try:
            for i, docs in enumerate(
                batch_iter(
                    yield_docs_from_geojson_stream(
                        f,
                        ndjson=args.ndjson,
                        geometry_field=args.geometry_field,
                        flatten_properties=args.flatten_properties,
                        keep_id=args.keep_feature_id,
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
            logging.exception("Failed to load GeoJSON: %s", e)
            raise

    logging.info(
        "Done. Inserted %d documents into %s.%s", total, args.db, args.collection
    )
    logging.info(
        "Reminder: MongoDB expects WGS84 coordinates (EPSG:4326) for 2dsphere."
    )


if __name__ == "__main__":
    main()
