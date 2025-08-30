#!/usr/bin/env python3
"""
Load CSV into a MongoDB collection.

Features:
- Streams the CSV (no need to load whole file into memory)
- Converts empty strings to None
- Tries to parse ints/floats automatically
- Optional: normalize column names to snake_case (spaces -> underscores, lowercased)
- Batched inserts with progress logging
- Works with .csv or .csv.gz

Usage:
  python load_pluto_csv_to_mongo.py \
      --csv /path/to/file.csv \
      --mongo-uri "mongodb://localhost:27017" \
      --db mydb \
      --collection pluto \
      --batch-size 5000 \
      --normalize-keys \
      --drop
"""

import argparse
import csv
import gzip
import io
import logging
import os
import re
from typing import Any, Dict, Iterable, List

from dotenv import load_dotenv
from pymongo import InsertOne, MongoClient

# ------------------------
# Helpers
# ------------------------


def normalize_key(k: str) -> str:
    k = k.strip().lower()
    k = re.sub(r"[^\w\s]", "", k)
    k = re.sub(r"\s+", "_", k)
    return k


def coerce_value(v: str) -> Any:
    if v is None or v.strip() == "":
        return None
    v = v.strip()

    try:
        if v.isdigit() and not (len(v) > 1 and v.startswith("0")):
            return int(v)
        return float(v.replace(",", ""))
    except ValueError:
        return v


def open_maybe_gzip(path: str, encoding: str = "utf-8") -> io.TextIOBase:
    return (
        io.TextIOWrapper(gzip.open(path, "rb"), encoding=encoding)
        if path.endswith(".gz")
        else open(path, "r", encoding=encoding)
    )


def read_csv_rows(
    csv_path: str, normalize_keys: bool = False, encoding: str = "utf-8"
) -> Iterable[Dict[str, Any]]:
    with open_maybe_gzip(csv_path, encoding=encoding) as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("Missing CSV headers")

        field_map = {
            name: normalize_key(name) if normalize_keys else name
            for name in reader.fieldnames
        }

        for row in reader:
            yield {field_map[k]: coerce_value(v) for k, v in row.items()}


def batch(iterable: Iterable[Dict[str, Any]], n: int) -> Iterable[List[Dict[str, Any]]]:
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


# ------------------------
# Main
# ------------------------


def main():
    load_dotenv()  # Load .env file

    parser = argparse.ArgumentParser(description="Load CSV into MongoDB collection.")
    parser.add_argument("--csv", required=True, help="Path to CSV (.csv or .csv.gz)")
    parser.add_argument("--db", required=True, help="Target database name")
    parser.add_argument("--collection", required=True, help="Target collection name")
    parser.add_argument(
        "--batch-size", type=int, default=5000, help="Insert batch size"
    )
    parser.add_argument(
        "--normalize-keys",
        action="store_true",
        help="Normalize column names to snake_case",
    )
    parser.add_argument(
        "--drop", action="store_true", help="Drop existing collection before loading"
    )
    parser.add_argument("--encoding", default="utf-8", help="CSV file encoding")
    args = parser.parse_args()

    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise EnvironmentError(
            "MONGO_URI not found in environment. Please define it in your .env file."
        )

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    client = MongoClient(mongo_uri)
    db = client[args.db]
    coll = db[args.collection]

    if args.drop:
        logging.info("Dropping collection: %s.%s", args.db, args.collection)
        coll.drop()

    total_inserted = 0
    logging.info("Loading from %s into %s.%s", args.csv, args.db, args.collection)

    for i, rows in enumerate(
        batch(
            read_csv_rows(args.csv, args.normalize_keys, args.encoding), args.batch_size
        ),
        start=1,
    ):
        result = coll.bulk_write([InsertOne(doc) for doc in rows], ordered=False)
        total_inserted += result.inserted_count
        logging.info(
            "Batch %d inserted %d (total %d)", i, result.inserted_count, total_inserted
        )

    logging.info("✅ Done. Inserted total of %d documents.", total_inserted)


if __name__ == "__main__":
    main()
