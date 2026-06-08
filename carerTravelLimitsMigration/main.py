"""
Carer Travel Limits Migration
Derives max_distance_km (home-point legs) and max_p2p_distance_km (point-to-point legs)
from VisitExport daily routes and travel_distances table.
"""

from __future__ import annotations

import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import OperationalError, InterfaceError
except ImportError:
    psycopg2 = None
    RealDictCursor = None
    execute_values = None
    OperationalError = Exception
    InterfaceError = Exception

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

from feasible_pairs_migration.feasible_pairs_migration import (
    connect_to_database,
    get_actual_employee_name,
    get_db_config,
    is_valid_feasibility_row,
    load_clients_lookup,
    load_users_lookup,
    parse_full_name,
    parse_visit_datetime,
    safe_strip,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("carer_travel_limits_migration.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

MIN_ROUTE_SAMPLES = 2
HISTOGRAM_BINS = 10

USER_TRAVEL_METHOD_TO_DB = {
    "Car": "car",
    "Bike": "bike",
    "Walk": "walk",
    "PublicTransport": "car",
}


def modal_histogram_midpoint(values: List[float], num_bins: int = HISTOGRAM_BINS) -> Optional[float]:
    """
    Build a histogram with num_bins and return the midpoint of the modal bin.
    Returns None when there are fewer than MIN_ROUTE_SAMPLES values.
    """
    if len(values) < MIN_ROUTE_SAMPLES:
        return None
    if num_bins < 1:
        num_bins = HISTOGRAM_BINS

    min_val = min(values)
    max_val = max(values)
    if max_val <= min_val:
        return round(float(min_val), 2)

    bin_width = (max_val - min_val) / num_bins
    counts = [0] * num_bins
    for value in values:
        idx = min(int((value - min_val) / bin_width), num_bins - 1)
        counts[idx] += 1

    max_count = max(counts)
    modal_idx = counts.index(max_count)
    midpoint = min_val + (modal_idx + 0.5) * bin_width
    return round(midpoint, 2)


def map_user_travel_method(travel_method: Optional[str]) -> str:
    if not travel_method:
        return "car"
    return USER_TRAVEL_METHOD_TO_DB.get(travel_method, "car")


def load_caregiver_travel_methods(connection) -> Dict[int, str]:
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT id, travel_method
            FROM "user"
            WHERE deleted_at IS NULL AND is_caregiver = true
        """)
        return {
            row["id"]: map_user_travel_method(row.get("travel_method"))
            for row in cursor.fetchall()
        }
    finally:
        cursor.close()


def load_travel_distance_map(connection) -> Dict[Tuple[str, int, str, int, str], int]:
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT from_type, from_id, to_type, to_id, travel_method, distance_meters
            FROM travel_distances
            WHERE distance_meters IS NOT NULL
        """)
        return {
            (
                row["from_type"],
                int(row["from_id"]),
                row["to_type"],
                int(row["to_id"]),
                row["travel_method"],
            ): int(row["distance_meters"])
            for row in cursor.fetchall()
        }
    finally:
        cursor.close()


def lookup_distance_km(
    distance_map: Dict[Tuple[str, int, str, int, str], int],
    from_type: str,
    from_id: int,
    to_type: str,
    to_id: int,
    travel_method: str,
) -> Optional[float]:
    meters = distance_map.get((from_type, from_id, to_type, to_id, travel_method))
    if meters is None:
        return None
    return meters / 1000.0


def extract_daily_routes_from_csv(
    csv_path,
    users_lookup,
    clients_lookup,
) -> Tuple[Dict[Tuple[int, datetime.date], List[int]], dict]:
    """
    Parse VisitExport and group ordered client IDs by (caregiver_id, visit_date).
    Uses all Personal Care rows (no roster window filter).
    """
    daily_routes: Dict[Tuple[int, datetime.date], List[int]] = defaultdict(list)
    stats = {
        "total_rows": 0,
        "valid_visits": 0,
        "daily_routes": 0,
        "skipped_non_personal_care": 0,
        "skipped_missing_carer": 0,
        "skipped_invalid_datetime": 0,
        "unmatched_caregivers": set(),
        "unmatched_clients": set(),
    }

    def get_service_location_name(row):
        return safe_strip(row.get("Service Location Name", ""))

    with open(csv_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            stats["total_rows"] += 1
            if not is_valid_feasibility_row(row):
                stats["skipped_non_personal_care"] += 1
                continue

            employee_name = get_actual_employee_name(row)
            service_location_name = get_service_location_name(row)
            if not employee_name:
                stats["skipped_missing_carer"] += 1
                continue
            if not service_location_name:
                continue

            visit_start = parse_visit_datetime(row)
            if not visit_start:
                stats["skipped_invalid_datetime"] += 1
                continue

            employee_first, employee_last = parse_full_name(employee_name)
            client_first, client_last = parse_full_name(service_location_name)
            if not employee_first or not employee_last:
                stats["unmatched_caregivers"].add(employee_name)
                continue
            if not client_first or not client_last:
                stats["unmatched_clients"].add(service_location_name)
                continue

            caregiver_key = (employee_first.lower(), employee_last.lower())
            caregiver_id = users_lookup.get(caregiver_key)
            if not caregiver_id:
                first_part = employee_first.split()[0] if employee_first else ""
                caregiver_id = users_lookup.get((first_part.lower(), employee_last.lower()))

            client_key = (client_first.lower(), client_last.lower())
            client_id = clients_lookup.get(client_key)
            if not client_id:
                first_part = client_first.split()[0] if client_first else ""
                client_id = clients_lookup.get((first_part.lower(), client_last.lower()))

            if not caregiver_id:
                stats["unmatched_caregivers"].add(employee_name)
                continue
            if not client_id:
                stats["unmatched_clients"].add(service_location_name)
                continue

            route_key = (caregiver_id, visit_start.date())
            daily_routes[route_key].append((visit_start, client_id))
            stats["valid_visits"] += 1

    ordered_routes: Dict[Tuple[int, datetime.date], List[int]] = {}
    for route_key, visits in daily_routes.items():
        visits.sort(key=lambda item: item[0])
        ordered_routes[route_key] = [client_id for _, client_id in visits]

    stats["daily_routes"] = len(ordered_routes)
    return ordered_routes, stats


def collect_carer_distance_samples(
    daily_routes: Dict[Tuple[int, datetime.date], List[int]],
    caregiver_methods: Dict[int, str],
    distance_map: Dict[Tuple[str, int, str, int, str], int],
) -> Tuple[Dict[int, List[float]], Dict[int, List[float]], dict]:
    hp_samples: Dict[int, List[float]] = defaultdict(list)
    pp_samples: Dict[int, List[float]] = defaultdict(list)
    stats = {
        "hp_legs_found": 0,
        "hp_legs_missing": 0,
        "pp_legs_found": 0,
        "pp_legs_missing": 0,
        "routes_processed": 0,
    }

    for (caregiver_id, _visit_date), client_ids in daily_routes.items():
        if not client_ids:
            continue
        stats["routes_processed"] += 1
        travel_method = caregiver_methods.get(caregiver_id, "car")

        first_client = client_ids[0]
        last_client = client_ids[-1]

        hp_to = lookup_distance_km(
            distance_map, "user", caregiver_id, "client", first_client, travel_method,
        )
        if hp_to is not None:
            hp_samples[caregiver_id].append(hp_to)
            stats["hp_legs_found"] += 1
        else:
            stats["hp_legs_missing"] += 1

        hp_from = lookup_distance_km(
            distance_map, "client", last_client, "user", caregiver_id, travel_method,
        )
        if hp_from is not None:
            hp_samples[caregiver_id].append(hp_from)
            stats["hp_legs_found"] += 1
        else:
            stats["hp_legs_missing"] += 1

        for prev_client, next_client in zip(client_ids, client_ids[1:]):
            if prev_client == next_client:
                continue
            pp_dist = lookup_distance_km(
                distance_map, "client", prev_client, "client", next_client, travel_method,
            )
            if pp_dist is not None:
                pp_samples[caregiver_id].append(pp_dist)
                stats["pp_legs_found"] += 1
            else:
                stats["pp_legs_missing"] += 1

    return hp_samples, pp_samples, stats


def update_carer_travel_limits(
    connection,
    hp_samples: Dict[int, List[float]],
    pp_samples: Dict[int, List[float]],
) -> Tuple[int, int]:
    updates = []
    for caregiver_id in set(hp_samples) | set(pp_samples):
        max_distance = modal_histogram_midpoint(hp_samples.get(caregiver_id, []))
        max_p2p = modal_histogram_midpoint(pp_samples.get(caregiver_id, []))
        if max_distance is None and max_p2p is None:
            continue
        updates.append((
            max_distance,
            max_p2p,
            caregiver_id,
        ))

    if not updates:
        logger.warning("No carers with enough HP/PP samples to update.")
        return 0, 0

    cursor = connection.cursor()
    try:
        for max_distance, max_p2p, caregiver_id in updates:
            cursor.execute(
                """
                UPDATE "user" SET
                    max_distance_km = COALESCE(%s, max_distance_km),
                    max_p2p_distance_km = COALESCE(%s, max_p2p_distance_km)
                WHERE id = %s
                """,
                (max_distance, max_p2p, caregiver_id),
            )
        connection.commit()
        with_both = sum(1 for max_d, max_p, _ in updates if max_d is not None and max_p is not None)
        logger.info("✓ Updated travel limits for %d caregiver(s)", len(updates))
        return len(updates), with_both
    except (OperationalError, InterfaceError) as e:
        connection.rollback()
        if ConnectionLostError:
            raise ConnectionLostError("carer_travel_limits", {}) from e
        raise
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def run(csv_path=None, connection_manager=None, state=None):
    if psycopg2 is None:
        logger.error("Missing psycopg2. Install: pip install psycopg2-binary")
        return False

    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Carer Travel Limits Migration                    ║
    ║   max_distance_km / max_p2p_distance_km from routes      ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    if state and state.is_completed("carer_travel_limits"):
        logger.info("Carer travel limits migration already completed (resume).")
        return True

    config = get_db_config()
    if not all([config["database"], config["user"], config["password"]]):
        logger.error("Missing database configuration in environment variables")
        return False

    if csv_path is not None:
        csv_path = Path(csv_path)
    elif len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        from migration_support import get_assets_dir
        csv_path = get_assets_dir() / "visit_data.csv"

    if not csv_path.exists():
        logger.error("CSV file not found: %s", csv_path)
        return False

    connection = None
    try:
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
            connection = connect_to_database(config)

        logger.info("Loading caregivers, clients, and travel_distances...")
        users_lookup = load_users_lookup(connection)
        clients_lookup = load_clients_lookup(connection)
        caregiver_methods = load_caregiver_travel_methods(connection)
        distance_map = load_travel_distance_map(connection)
        if not distance_map:
            logger.error("travel_distances table is empty — run Calculate distances first.")
            return False

        logger.info("Extracting daily routes from %s", csv_path)
        daily_routes, extract_stats = extract_daily_routes_from_csv(
            csv_path, users_lookup, clients_lookup,
        )
        if not daily_routes:
            logger.warning("No valid daily routes found in CSV.")
            return False

        hp_samples, pp_samples, sample_stats = collect_carer_distance_samples(
            daily_routes, caregiver_methods, distance_map,
        )
        updated_count, both_count = update_carer_travel_limits(connection, hp_samples, pp_samples)

        if state:
            state.update("carer_travel_limits", status="completed")
            state.clear_step("carer_travel_limits")

        print("\n" + "=" * 60)
        print("✓ CARER TRAVEL LIMITS MIGRATION COMPLETED")
        print("=" * 60)
        print(f"  CSV rows processed: {extract_stats['total_rows']}")
        print(f"  Valid visits: {extract_stats['valid_visits']}")
        print(f"  Daily routes: {extract_stats['daily_routes']}")
        print(f"  HP legs found / missing: {sample_stats['hp_legs_found']} / {sample_stats['hp_legs_missing']}")
        print(f"  PP legs found / missing: {sample_stats['pp_legs_found']} / {sample_stats['pp_legs_missing']}")
        print(f"  Caregivers updated: {updated_count} ({both_count} with both limits)")
        print(f"  Unmatched caregivers: {len(extract_stats['unmatched_caregivers'])}")
        print(f"  Unmatched clients: {len(extract_stats['unmatched_clients'])}")
        return updated_count > 0 or (hp_samples or pp_samples)

    except (OperationalError, InterfaceError) as e:
        if ConnectionLostError:
            raise ConnectionLostError("carer_travel_limits", {}) from e
        raise
    except Exception as e:
        logger.error("Migration error: %s", e, exc_info=True)
        return False
    finally:
        if connection and not connection_manager:
            connection.close()


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
