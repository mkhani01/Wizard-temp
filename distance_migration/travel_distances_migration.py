"""
Travel Distances Migration
Loads users (caregivers) and clients with lat/long from DB, computes travel distance/duration
via OSRM for each (user, client) pair and each travel method, then inserts or updates
the travel_distances table.
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from distance_migration.osrm import get_distance_matrix

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("travel_distances_migration.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# DB enum values (match TypeORM/travel-distance.enums)
ENTITY_TYPE_USER = "user"
ENTITY_TYPE_CLIENT = "client"
TRAVEL_METHODS = ["driving-car", "cycling-regular", "foot-walking"]
CALCULATION_STATUS_COMPLETED = "completed"
CALCULATION_STATUS_FAILED = "failed"
CALCULATION_STATUS_PENDING = "pending"


def get_db_config():
    """Get database configuration from environment variables."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }


def connect_to_database(config):
    """Connect to PostgreSQL database."""
    try:
        logger.info("Connecting to PostgreSQL...")
        connection = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            cursor_factory=RealDictCursor,
        )
        connection.autocommit = False
        logger.info("✓ Connected to database")
        return connection
    except Exception as e:
        logger.error(f"✗ Failed to connect: {e}")
        raise


def load_users_with_locations(connection):
    """Load caregivers (users) with non-null latitude/longitude. Returns dict id -> {latitude, longitude}."""
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT id, latitude, longitude
            FROM "user"
            WHERE deleted_at IS NULL AND is_caregiver = true
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        rows = cursor.fetchall()
        result = {}
        for row in rows:
            result[row["id"]] = {
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
            }
        logger.info(f"✓ Loaded {len(result)} users (caregivers) with coordinates")
        return result
    finally:
        cursor.close()


def load_clients_with_locations(connection):
    """Load clients with non-null latitude/longitude. Returns dict id -> {latitude, longitude}."""
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT id, latitude, longitude
            FROM client
            WHERE deleted_at IS NULL
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        rows = cursor.fetchall()
        result = {}
        for row in rows:
            result[row["id"]] = {
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
            }
        logger.info(f"✓ Loaded {len(result)} clients with coordinates")
        return result
    finally:
        cursor.close()


def upsert_travel_distance(cursor, from_type, from_id, to_type, to_id, travel_method, distance_meters, duration_minutes, status, error_message=None):
    """Insert or update a single travel_distances row. Matches on (from_type, from_id, to_type, to_id, travel_method)."""
    now = datetime.utcnow()
    cursor.execute("""
        SELECT id FROM travel_distances
        WHERE from_type = %s AND from_id = %s AND to_type = %s AND to_id = %s AND travel_method = %s
    """, (from_type, from_id, to_type, to_id, travel_method))
    row = cursor.fetchone()
    if row:
        cursor.execute("""
            UPDATE travel_distances
            SET distance_meters = %s, duration_minutes = %s, calculation_status = %s,
                error_message = %s, last_calculated_at = %s, updated_at = %s
            WHERE id = %s
        """, (distance_meters, duration_minutes, status, error_message, now, now, row["id"]))
    else:
        cursor.execute("""
            INSERT INTO travel_distances
            (from_type, from_id, from_hexagon, to_type, to_id, to_hexagon, travel_method,
             distance_meters, duration_minutes, calculation_status, error_message, last_calculated_at, created_at, updated_at)
            VALUES (%s, %s, NULL, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (from_type, from_id, to_type, to_id, travel_method, distance_meters, duration_minutes, status, error_message, now, now, now))


def run():
    """Main entry: load users and clients from DB, compute distances via OSRM, upsert travel_distances. Then verify."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Travel Distances Migration                      ║
    ║         User ↔ Client distances via OSRM → travel_distances
    ╚══════════════════════════════════════════════════════════╝
    """)

    config = get_db_config()
    if not all([config["database"], config["user"], config["password"]]):
        logger.error("Missing DB_NAME, DB_USER, DB_PASSWORD")
        return False

    connection = None
    try:
        connection = connect_to_database(config)
        users = load_users_with_locations(connection)
        clients = load_clients_with_locations(connection)

        if not users or not clients:
            logger.warning("No users with coordinates or no clients with coordinates. Nothing to migrate.")
            return True

        cursor = connection.cursor()
        step_size = 50
        inserted_or_updated = 0
        failed = 0

        for travel_method in TRAVEL_METHODS:
            logger.info("")
            logger.info("=" * 60)
            logger.info(f"Travel method: {travel_method}")
            logger.info("=" * 60)
            try:
                matrix_result = get_distance_matrix(
                    entities_info1=users,
                    entities_info2=clients,
                    travel_method=travel_method,
                    step_size=step_size,
                )
            except Exception as e:
                logger.exception("OSRM request failed for %s", travel_method)
                failed += 1
                continue

            dist_map = matrix_result["distance"]
            dur_map = matrix_result["duration"]

            for (user_id, client_id), dist_km in dist_map.items():
                dur_min = dur_map.get((user_id, client_id), 0)
                distance_meters = int(round(dist_km * 1000))
                duration_minutes = int(dur_min)
                try:
                    upsert_travel_distance(
                        cursor,
                        from_type=ENTITY_TYPE_USER,
                        from_id=user_id,
                        to_type=ENTITY_TYPE_CLIENT,
                        to_id=client_id,
                        travel_method=travel_method,
                        distance_meters=distance_meters,
                        duration_minutes=duration_minutes,
                        status=CALCULATION_STATUS_COMPLETED,
                        error_message=None,
                    )
                    inserted_or_updated += 1
                except Exception as e:
                    logger.warning("Upsert failed for user %s -> client %s (%s): %s", user_id, client_id, travel_method, e)
                    failed += 1

        connection.commit()
        logger.info("")
        logger.info(f"✓ Upserted {inserted_or_updated} travel_distances rows (failed: {failed})")

        verify_ok = verify_distances(connection, users, clients)
        if verify_ok:
            logger.info("✓ Verification passed: all expected distances are present.")
        else:
            logger.warning("Verification reported missing or inconsistent distances. Check logs above.")

        return True
    except Exception as e:
        if connection:
            connection.rollback()
        logger.exception("Migration failed: %s", e)
        return False
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")


def verify_distances(connection, users, clients):
    """Verify that for each (user, client) and each travel method we have a completed row in travel_distances."""
    expected_count = len(users) * len(clients) * len(TRAVEL_METHODS)
    if expected_count == 0:
        return True

    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) AS cnt FROM travel_distances
            WHERE from_type = %s AND to_type = %s AND calculation_status = %s
        """, (ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT, CALCULATION_STATUS_COMPLETED))
        row = cursor.fetchone()
        actual = row["cnt"] if row else 0
        missing = expected_count - actual
        logger.info("Verification: expected %s rows (user->client, all 3 methods), found %s", expected_count, actual)
        if missing > 0:
            logger.warning("  Missing %s distance records.", missing)
            return False
        return True
    finally:
        cursor.close()


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
