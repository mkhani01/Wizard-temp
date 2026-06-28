"""
Geocode users/clients JSON files and export OSRM travel matrices.

This is a standalone CLI utility. It reads JSON backup files, writes enriched
copies with coordinates, and creates distance/duration matrices without using
the database or wizard state.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv

from distance_migration.osrm import get_distance_matrix
from geocodeCalculation.geocoder import GeocodeCache, GoogleGeocoder

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


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _google_api_key() -> str:
    api_key = (os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_KEY") or "").strip()
    if not api_key:
        raise RuntimeError(
            "Google Maps API key not set. Use GOOGLE_MAPS_API_KEY or GOOGLE_KEY in the environment."
        )
    return api_key


def _read_json_array(json_path: Path, key: str) -> List[dict]:
    with open(json_path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    rows = data.get(key)
    if not isinstance(rows, list):
        raise RuntimeError(f"{json_path} must contain a top-level {key!r} array.")
    return rows


def _write_json_array(json_path: Path, key: str, rows: List[dict]) -> None:
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump({key: rows}, handle, indent=2, ensure_ascii=False)


def _clean_postcode(value) -> str:
    return str(value or "").strip()


def _entity_label(row: dict) -> str:
    name = str(row.get("name") or "").strip()
    lastname = str(row.get("lastname") or "").strip()
    entity_id = row.get("id")
    full_name = " ".join(part for part in (name, lastname) if part)
    if full_name:
        return f"{full_name} (ID: {entity_id})"
    return f"ID: {entity_id}"


def _valid_existing_coordinate(row: dict, field: str) -> float | None:
    value = row.get(field)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _geocode_postcode(geocoder: GoogleGeocoder, postcode: str) -> Tuple[float, float]:
    result = geocoder.geocode(postcode)
    if not result or not result.get("results"):
        raise RuntimeError(f"Failed to geocode postcode: {postcode!r}")
    location = result["results"][0]["geometry"]["location"]
    return float(location["lat"]), float(location["lng"])


def _geocode_rows(
    rows: List[dict],
    entity_type: str,
    geocoder: GoogleGeocoder,
) -> Tuple[List[dict], Dict[int, Dict[str, float]]]:
    enriched_rows: List[dict] = []
    locations: Dict[int, Dict[str, float]] = {}
    skipped = 0

    for index, row in enumerate(rows, start=1):
        entity_id = row.get("id")
        if entity_id is None:
            skipped += 1
            logger.warning("Skipping %s row %s with no id.", entity_type, index)
            enriched_rows.append(dict(row))
            continue
        try:
            numeric_id = int(entity_id)
        except (TypeError, ValueError):
            skipped += 1
            logger.warning("Skipping %s row %s with invalid id: %r.", entity_type, index, entity_id)
            enriched_rows.append(dict(row))
            continue

        enriched = dict(row)
        latitude = _valid_existing_coordinate(enriched, "latitude")
        longitude = _valid_existing_coordinate(enriched, "longitude")

        if latitude is None or longitude is None:
            postcode = _clean_postcode(enriched.get("postcode"))
            if not postcode:
                skipped += 1
                logger.warning("Skipping %s %s with no postcode.", entity_type, _entity_label(enriched))
                enriched_rows.append(enriched)
                continue
            latitude, longitude = _geocode_postcode(geocoder, postcode)
            enriched["latitude"] = latitude
            enriched["longitude"] = longitude
            logger.info(
                "Geocoded %s %s (%s) -> %.6f, %.6f",
                entity_type,
                _entity_label(enriched),
                postcode,
                latitude,
                longitude,
            )
        else:
            enriched["latitude"] = latitude
            enriched["longitude"] = longitude
            logger.info(
                "Using existing coordinates for %s %s -> %.6f, %.6f",
                entity_type,
                _entity_label(enriched),
                latitude,
                longitude,
            )

        locations[numeric_id] = {"latitude": latitude, "longitude": longitude}
        enriched_rows.append(enriched)

    logger.info(
        "Prepared %s %s locations; skipped %s rows.",
        len(locations),
        entity_type,
        skipped,
    )
    return enriched_rows, locations


def _merge_matrices(
    user_locations: Dict[int, Dict[str, float]],
    client_locations: Dict[int, Dict[str, float]],
    travel_method: str,
    step_size: int,
) -> Dict[str, Dict[str, float | int]]:
    segments = (
        get_distance_matrix(
            entities_info1=user_locations,
            entities_info2=user_locations,
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
            entities_info1=user_locations,
            entities_info2=client_locations,
            travel_method=travel_method,
            step_size=step_size,
        ),
        get_distance_matrix(
            entities_info1=client_locations,
            entities_info2=user_locations,
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


def run(
    users_json: Path,
    clients_json: Path,
    output_dir: Path,
    step_size: int = 50,
) -> bool:
    output_dir.mkdir(parents=True, exist_ok=True)

    users = _read_json_array(users_json, "user")
    clients = _read_json_array(clients_json, "client")
    logger.info("Loaded %s users and %s clients.", len(users), len(clients))

    geocoder = GoogleGeocoder(_google_api_key(), GeocodeCache())
    enriched_users, user_locations = _geocode_rows(users, "user", geocoder)
    enriched_clients, client_locations = _geocode_rows(clients, "client", geocoder)

    users_output = output_dir / "users_with_coordinates.json"
    clients_output = output_dir / "clients_with_coordinates.json"

    _write_json_array(users_output, "user", enriched_users)
    _write_json_array(clients_output, "client", enriched_clients)
    logger.info("Wrote %s", users_output)
    logger.info("Wrote %s", clients_output)

    if not user_locations or not client_locations:
        raise RuntimeError("Cannot calculate distances without at least one user and one client location.")

    for travel_method, filename in TRAVEL_METHODS:
        logger.info("Computing %s distances...", travel_method)
        matrix = _merge_matrices(
            user_locations=user_locations,
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
        description="Geocode users.json and clients.json, then export OSRM travel matrices.",
    )
    parser.add_argument(
        "--users",
        type=Path,
        default=root / "users.json",
        help="Path to users JSON (default: project users.json)",
    )
    parser.add_argument(
        "--clients",
        type=Path,
        default=root / "clients.json",
        help="Path to clients JSON (default: project clients.json)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=root / "output",
        help="Output directory for enriched JSONs and travel matrix JSONs",
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
            users_json=args.users.resolve(),
            clients_json=args.clients.resolve(),
            output_dir=args.output.resolve(),
            step_size=args.step_size,
        )
    except Exception as exc:
        logger.error("%s", exc)
        return 1
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
