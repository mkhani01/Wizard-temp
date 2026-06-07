"""
Geocode carer/customer CSV postcodes and export OSRM travel matrices.

Reads Post Code from carer.csv and customers.csv, resolves coordinates via the
shared Google geocoder, then writes enriched CSVs plus walking/driving/cycling JSON.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from distance_migration.osrm import get_distance_matrix
from encoding_utils import fix_utf8_mojibake
from geocodeCalculation.geocoder import GeocodeCache, GoogleGeocoder

CAREGIVER_ID = 2000
TRAVEL_METHODS = (
    ("foot-walking", "walking_data.json"),
    ("driving-car", "driving_data.json"),
    ("cycling-regular", "cycling_data.json"),
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def _clean_excel_value(value) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if text.startswith('="') and text.endswith('"'):
        text = text[2:-1]
    elif text.startswith("=") and text.endswith('"'):
        text = text[1:].strip('"')
    return text.strip()


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _google_api_key() -> str:
    api_key = (os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            "Google Maps API key not set. Use GOOGLE_MAPS_API_KEY or GOOGLE_KEY in the environment."
        )
    return api_key


def _read_csv_rows(csv_path: Path) -> Tuple[List[str], List[dict]]:
    with open(csv_path, "r", encoding="utf-8") as handle:
        sample = handle.read(2048)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(handle, dialect=dialect)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def _write_csv_rows(csv_path: Path, fieldnames: List[str], rows: List[dict]) -> None:
    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _normalize_postcode(value: str) -> str:
    return _clean_excel_value(fix_utf8_mojibake(value)).strip()


def _parse_service_location_id(value, row_number: int) -> int | None:
    raw_id = _clean_excel_value(value)
    if not raw_id:
        logger.warning("Skipping customer row %s with no Service Location ID.", row_number)
        return None
    if not raw_id.isdigit():
        logger.warning(
            "Skipping customer row %s with invalid Service Location ID: %r",
            row_number,
            raw_id[:120],
        )
        return None
    return int(raw_id)


def _geocode_postcode(geocoder: GoogleGeocoder, postcode: str) -> Tuple[float, float]:
    result = geocoder.geocode(postcode)
    if not result or not result.get("results"):
        raise RuntimeError(f"Failed to geocode postcode: {postcode!r}")
    location = result["results"][0]["geometry"]["location"]
    return float(location["lat"]), float(location["lng"])


def _build_location_map(
    geocoder: GoogleGeocoder,
    postcode_by_id: Dict[int, str],
) -> Dict[int, Dict[str, float]]:
    locations: Dict[int, Dict[str, float]] = {}
    for entity_id, postcode in postcode_by_id.items():
        latitude, longitude = _geocode_postcode(geocoder, postcode)
        locations[entity_id] = {"latitude": latitude, "longitude": longitude}
        logger.info("Geocoded ID %s (%s) -> %.6f, %.6f", entity_id, postcode, latitude, longitude)
    return locations


def _enrich_rows(
    rows: List[dict],
    fieldnames: List[str],
    postcode_column: str,
    locations: Dict[int, Dict[str, float]],
    entity_id_for_row,
) -> Tuple[List[str], List[dict]]:
    output_fields = list(fieldnames)
    for column in ("latitude", "longitude"):
        if column not in output_fields:
            output_fields.append(column)

    enriched_rows: List[dict] = []
    for row in rows:
        entity_id = entity_id_for_row(row)
        postcode = _normalize_postcode(row.get(postcode_column, ""))
        coords = locations[entity_id]
        enriched = dict(row)
        enriched[postcode_column] = postcode
        enriched["latitude"] = coords["latitude"]
        enriched["longitude"] = coords["longitude"]
        enriched_rows.append(enriched)
    return output_fields, enriched_rows


def _merge_matrices(
    caregiver_locations: Dict[int, Dict[str, float]],
    client_locations: Dict[int, Dict[str, float]],
    travel_method: str,
    step_size: int,
) -> Dict[str, Dict[str, float | int]]:
    segments = (
        get_distance_matrix(
            entities_info1=caregiver_locations,
            entities_info2=caregiver_locations,
            travel_method=travel_method,
            step_size=step_size,
        ),
        get_distance_matrix(
            entities_info1=client_locations,
            entities_info2=client_locations,
            travel_method=travel_method,
            step_size=step_size,
        ),
        get_distance_matrix(
            entities_info1=caregiver_locations,
            entities_info2=client_locations,
            travel_method=travel_method,
            step_size=step_size,
        ),
        get_distance_matrix(
            entities_info1=client_locations,
            entities_info2=caregiver_locations,
            travel_method=travel_method,
            step_size=step_size,
        ),
    )

    distance: Dict[str, float] = {}
    duration: Dict[str, int] = {}
    for segment in segments:
        for (from_id, to_id), value in segment["distance"].items():
            distance[f"{from_id}_{to_id}"] = value
        for (from_id, to_id), value in segment["duration"].items():
            duration[f"{from_id}_{to_id}"] = value
    return {"distance": distance, "duration": duration}


def _load_carer_postcode(rows: List[dict]) -> Dict[int, str]:
    if not rows:
        raise RuntimeError("carer.csv is empty.")
    if len(rows) > 1:
        logger.warning("Multiple carer rows found; using the first row and assigning ID %s.", CAREGIVER_ID)

    postcode = _normalize_postcode(rows[0].get("Post Code", ""))
    if not postcode:
        raise RuntimeError("Carer row is missing Post Code.")
    return {CAREGIVER_ID: postcode}


def _collect_customer_records(rows: List[dict]) -> List[Tuple[int, dict, int]]:
    records: List[Tuple[int, dict, int]] = []
    for index, row in enumerate(rows, start=2):
        entity_id = _parse_service_location_id(row.get("Service Location ID", ""), index)
        if entity_id is None:
            continue
        records.append((index, row, entity_id))
    if not records:
        raise RuntimeError("No customer rows with Service Location ID were found.")
    return records


def _load_customer_postcodes(records: List[Tuple[int, dict, int]]) -> Dict[int, str]:
    postcodes: Dict[int, str] = {}
    for index, row, entity_id in records:
        postcode = _normalize_postcode(row.get("Post Code", ""))
        if not postcode:
            raise RuntimeError(f"Customer ID {entity_id} is missing Post Code (CSV row {index}).")
        if entity_id in postcodes and postcodes[entity_id] != postcode:
            raise RuntimeError(
                f"Customer ID {entity_id} has conflicting postcodes in the CSV."
            )
        postcodes[entity_id] = postcode
    return postcodes


def run(
    carer_csv: Path,
    customers_csv: Path,
    output_dir: Path,
    step_size: int = 50,
) -> bool:
    output_dir.mkdir(parents=True, exist_ok=True)

    carer_fields, carer_rows = _read_csv_rows(carer_csv)
    customer_fields, customer_rows = _read_csv_rows(customers_csv)

    geocoder = GoogleGeocoder(_google_api_key(), GeocodeCache())
    caregiver_postcodes = _load_carer_postcode(carer_rows)
    customer_records = _collect_customer_records(customer_rows)
    customer_postcodes = _load_customer_postcodes(customer_records)

    caregiver_locations = _build_location_map(geocoder, caregiver_postcodes)
    client_locations = _build_location_map(geocoder, customer_postcodes)

    carer_output_fields, carer_output_rows = _enrich_rows(
        carer_rows,
        carer_fields,
        postcode_column="Post Code",
        locations=caregiver_locations,
        entity_id_for_row=lambda _row: CAREGIVER_ID,
    )

    customer_output_fields, customer_output_rows = _enrich_rows(
        [row for _index, row, entity_id in customer_records if entity_id in client_locations],
        customer_fields,
        postcode_column="Post Code",
        locations=client_locations,
        entity_id_for_row=lambda row: int(_clean_excel_value(row.get("Service Location ID", ""))),
    )

    carer_output_path = output_dir / "carer_with_coordinates.csv"
    customers_output_path = output_dir / "customers_with_coordinates.csv"
    _write_csv_rows(carer_output_path, carer_output_fields, carer_output_rows)
    _write_csv_rows(customers_output_path, customer_output_fields, customer_output_rows)
    logger.info("Wrote %s", carer_output_path)
    logger.info("Wrote %s", customers_output_path)

    for travel_method, filename in TRAVEL_METHODS:
        logger.info("Computing %s distances...", travel_method)
        matrix = _merge_matrices(
            caregiver_locations=caregiver_locations,
            client_locations=client_locations,
            travel_method=travel_method,
            step_size=step_size,
        )
        output_path = output_dir / filename
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(matrix, handle, indent=2)
        logger.info(
            "Wrote %s (%s distance pairs, %s duration pairs)",
            output_path,
            len(matrix["distance"]),
            len(matrix["duration"]),
        )

    logger.info(
        "Geocode API requests: %s, cache hits: %s",
        geocoder.request_count,
        geocoder.cache_hits,
    )
    return True


def _parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    root = _project_root()
    parser = argparse.ArgumentParser(
        description="Geocode carer/customer CSV postcodes and export OSRM travel matrices.",
    )
    parser.add_argument(
        "--carer",
        type=Path,
        default=root / "carer.csv",
        help="Path to carer CSV (default: project carer.csv)",
    )
    parser.add_argument(
        "--customers",
        type=Path,
        default=root / "customers.csv",
        help="Path to customers CSV (default: project customers.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "output",
        help="Output directory for enriched CSVs and JSON matrices",
    )
    parser.add_argument(
        "--step-size",
        type=int,
        default=50,
        help="OSRM matrix batch size (default: 50)",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    load_dotenv(_project_root() / ".env")
    args = _parse_args(argv)
    try:
        success = run(
            carer_csv=args.carer.resolve(),
            customers_csv=args.customers.resolve(),
            output_dir=args.output.resolve(),
            step_size=args.step_size,
        )
    except Exception as exc:
        logger.error("%s", exc)
        return 1
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
