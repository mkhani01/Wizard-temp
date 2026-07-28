import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
import psycopg2
from tqdm import tqdm

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

DATABASE_DSN = os.getenv("DATABASE_DSN")
TRAVEL_METHOD = os.getenv("TRAVEL_METHOD", "car")  # car | bike | walk
OUTPUT_FILE = os.getenv("OUTPUT_FILE", f"{TRAVEL_METHOD}_data.json")


def fetch_ids_and_rows(conn, travel_method: str):
    ids_sql = """
    WITH ids AS (
      SELECT from_id AS id
      FROM travel_distances
      WHERE travel_method = %s
        AND calculation_status = 'completed'

      UNION

      SELECT to_id AS id
      FROM travel_distances
      WHERE travel_method = %s
        AND calculation_status = 'completed'
    )
    SELECT id
    FROM ids
    ORDER BY id;
    """

    rows_sql = """
    SELECT
      LEAST(from_id, to_id) AS a,
      GREATEST(from_id, to_id) AS b,
      CASE
        WHEN distance_meters IS NULL THEN NULL
        ELSE ROUND((distance_meters / 1000.0)::numeric, 4)::float8
      END AS distance_km,
      duration_minutes
    FROM travel_distances
    WHERE travel_method = %s
      AND calculation_status = 'completed';
    """

    with conn.cursor() as cur:
        cur.execute(ids_sql, (travel_method, travel_method))
        ids = [row[0] for row in cur.fetchall()]

    lookup: Dict[Tuple[int, int], Tuple[Optional[float], Optional[int]]] = {}
    with conn.cursor(name="distance_cursor") as cur:
        cur.itersize = 50000
        cur.execute(rows_sql, (travel_method,))
        for a, b, distance_km, duration_minutes in tqdm(
            cur,
            desc="Loading travel distances",
            unit="rows",
        ):
            lookup[(a, b)] = (distance_km, duration_minutes)

    return ids, lookup


def write_distance_section(f, ids: List[int], lookup):
    f.write('{"distance":{')
    first = True
    total = len(ids) * len(ids)

    with tqdm(total=total, desc="Writing distance matrix", unit="pairs") as pbar:
        for id1 in ids:
            for id2 in ids:
                if not first:
                    f.write(",")
                first = False

                key = f"{id1}_{id2}"
                f.write(json.dumps(key))
                f.write(":")

                if id1 == id2:
                    f.write("0")
                else:
                    a, b = (id1, id2) if id1 < id2 else (id2, id1)
                    value = lookup.get((a, b))
                    if value is None or value[0] is None:
                        f.write("null")
                    else:
                        f.write(str(value[0]))

                pbar.update(1)


def write_duration_section(f, ids: List[int], lookup):
    f.write('},"duration":{')
    first = True
    total = len(ids) * len(ids)

    with tqdm(total=total, desc="Writing duration matrix", unit="pairs") as pbar:
        for id1 in ids:
            for id2 in ids:
                if not first:
                    f.write(",")
                first = False

                key = f"{id1}_{id2}"
                f.write(json.dumps(key))
                f.write(":")

                if id1 == id2:
                    f.write("0")
                else:
                    a, b = (id1, id2) if id1 < id2 else (id2, id1)
                    value = lookup.get((a, b))
                    if value is None or value[1] is None:
                        f.write("null")
                    else:
                        f.write(str(value[1]))

                pbar.update(1)

    f.write("}}")


def main():
    if not DATABASE_DSN:
        print("Missing DATABASE_DSN env var.", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(DATABASE_DSN)
    try:
        ids, lookup = fetch_ids_and_rows(conn, TRAVEL_METHOD)
        print(f"Distinct IDs: {len(ids)}")
        print(f"Stored completed pairs: {len(lookup)}")
        print(f"Total matrix cells per section: {len(ids) * len(ids)}")

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            write_distance_section(f, ids, lookup)
            write_duration_section(f, ids, lookup)

        print(f"Export written to: {OUTPUT_FILE}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()