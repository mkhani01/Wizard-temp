"""
Travel Distances Migration (Fast Bulk Version)
Loads users and clients, computes distances via OSRM, then performs a BULK INSERT.
- Wipes existing travel_distances table before starting.
- Computes: user→user, client→client, user→client, client→user (separate requests).
- Uses execute_values for massive speedup (100x faster than row-by-row).
- Maps OSRM profiles (driving-car) to DB enums (car).
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

# Import OSRM helper
try:
    from distance_migration.osrm import get_distance_matrix
except ImportError:
    import importlib.util
    spec = importlib.util.spec_from_file_location("osrm", os.path.join(os.path.dirname(__file__), "osrm.py"))
    osrm_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(osrm_module)
    get_distance_matrix = osrm_module.get_distance_matrix

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("travel_distances_migration.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# DB Enum values
ENTITY_TYPE_USER = "user"
ENTITY_TYPE_CLIENT = "client"
CALCULATION_STATUS_COMPLETED = "completed"

# MAP: OSRM Profile Name -> Database Enum Value
TRAVEL_METHOD_MAP = {
    "driving-car": "car",
    "cycling-regular": "bike",
    "foot-walking": "walk"
}
OSRM_METHODS = list(TRAVEL_METHOD_MAP.keys())

# Batch size for OSRM API calls
DEFAULT_STEP_SIZE = 25
# Batch size for Database Bulk Insert (number of rows per INSERT)
DB_INSERT_BATCH_SIZE = 5000


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


def run():
    """
    Main entry:
    1. Wipe travel_distances table.
    2. Compute distances via OSRM.
    3. Bulk insert results.
    """
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     Travel Distances Migration (FAST BULK MODE)          ║
    ║     Wipe -> Calculate -> Bulk Insert                     ║
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

        if not users and not clients:
            logger.warning("No users and no clients with coordinates. Nothing to migrate.")
            return True

        cursor = connection.cursor()
        now = datetime.utcnow()

        # --- STEP 1: WIPE OLD DATA ---
        logger.warning("Clearing existing travel_distances table...")
        cursor.execute("TRUNCATE TABLE travel_distances RESTART IDENTITY;")
        connection.commit()
        logger.info("✓ Table cleared.")

        total_expected = (
            len(users) ** 2 * len(OSRM_METHODS)
            + len(clients) ** 2 * len(OSRM_METHODS)
            + len(users) * len(clients) * len(OSRM_METHODS) * 2  # user→client and client→user
        )
        logger.info(f"Total expected distance records: {total_expected}")

        # --- STEP 2: CALCULATE & BULK INSERT (user↔user, client↔client, user↔client, client↔user) ---
        batch_data = []
        processed_count = 0

        def flush_batch():
            nonlocal batch_data, processed_count
            if not batch_data:
                return
            insert_batch(cursor, batch_data)
            connection.commit()
            logger.info(f"  Inserted {processed_count} records so far...")
            batch_data = []

        def add_rows(dist_map, dur_map, from_type, to_type, db_enum_method):
            nonlocal batch_data, processed_count
            skipped_null = 0
            for (from_id, to_id), dist_km in dist_map.items():
                if dist_km is None:
                    skipped_null += 1
                    logger.debug(
                        "SKIPPED - null distance | from_type=%s from_id=%s to_type=%s to_id=%s method=%s",
                        from_type, from_id, to_type, to_id, db_enum_method
                    )
                    continue
                dur_min = dur_map.get((from_id, to_id), 0)
                distance_meters = int(round(dist_km * 1000))
                duration_minutes = int(dur_min) if dur_min else 0
                row_data = (
                    from_type,
                    from_id,
                    to_type,
                    to_id,
                    db_enum_method,
                    distance_meters,
                    duration_minutes,
                    CALCULATION_STATUS_COMPLETED,
                    None,
                    now,
                    now,
                    now,
                )
                batch_data.append(row_data)
                processed_count += 1
                if len(batch_data) >= DB_INSERT_BATCH_SIZE:
                    flush_batch()
            if skipped_null:
                logger.info("  Skipped %d pairs with null distance (%s→%s, %s)", skipped_null, from_type, to_type, db_enum_method)

        for osrm_method in OSRM_METHODS:
            db_enum_method = TRAVEL_METHOD_MAP[osrm_method]
            logger.info("")
            logger.info("=" * 60)
            logger.info(f"Travel method: {osrm_method} (saving as '{db_enum_method}')")
            logger.info("=" * 60)

            # 1. User → User
            if users:
                try:
                    matrix_result = get_distance_matrix(
                        entities_info1=users,
                        entities_info2=users,
                        travel_method=osrm_method,
                        step_size=DEFAULT_STEP_SIZE,
                    )
                    add_rows(
                        matrix_result["distance"],
                        matrix_result["duration"],
                        ENTITY_TYPE_USER,
                        ENTITY_TYPE_USER,
                        db_enum_method,
                    )
                    logger.info("  ✓ User → User")
                except Exception as e:
                    logger.exception("OSRM user→user failed for %s: %s", osrm_method, e)

            # 2. Client → Client
            if clients:
                try:
                    matrix_result = get_distance_matrix(
                        entities_info1=clients,
                        entities_info2=clients,
                        travel_method=osrm_method,
                        step_size=DEFAULT_STEP_SIZE,
                    )
                    add_rows(
                        matrix_result["distance"],
                        matrix_result["duration"],
                        ENTITY_TYPE_CLIENT,
                        ENTITY_TYPE_CLIENT,
                        db_enum_method,
                    )
                    logger.info("  ✓ Client → Client")
                except Exception as e:
                    logger.exception("OSRM client→client failed for %s: %s", osrm_method, e)

            # 3. User → Client
            if users and clients:
                try:
                    matrix_result = get_distance_matrix(
                        entities_info1=users,
                        entities_info2=clients,
                        travel_method=osrm_method,
                        step_size=DEFAULT_STEP_SIZE,
                    )
                    add_rows(
                        matrix_result["distance"],
                        matrix_result["duration"],
                        ENTITY_TYPE_USER,
                        ENTITY_TYPE_CLIENT,
                        db_enum_method,
                    )
                    logger.info("  ✓ User → Client")
                except Exception as e:
                    logger.exception("OSRM user→client failed for %s: %s", osrm_method, e)

            # 4. Client → User (separate request; direction matters)
            if users and clients:
                try:
                    matrix_result = get_distance_matrix(
                        entities_info1=clients,
                        entities_info2=users,
                        travel_method=osrm_method,
                        step_size=DEFAULT_STEP_SIZE,
                    )
                    add_rows(
                        matrix_result["distance"],
                        matrix_result["duration"],
                        ENTITY_TYPE_CLIENT,
                        ENTITY_TYPE_USER,
                        db_enum_method,
                    )
                    logger.info("  ✓ Client → User")
                except Exception as e:
                    logger.exception("OSRM client→user failed for %s: %s", osrm_method, e)

        # Insert any remaining rows
        flush_batch()

        logger.info("")
        logger.info("✓ Migration completed successfully.")
        
        # Verify
        cursor.execute("SELECT COUNT(*) AS cnt FROM travel_distances")
        total_rows = cursor.fetchone()["cnt"]
        logger.info(f"Total rows in table: {total_rows}")
        
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


def insert_batch(cursor, data):
    """
    Uses psycopg2.execute_values for extremely fast bulk inserts.
    """
    sql = """
        INSERT INTO travel_distances
        (from_type, from_id, from_hexagon, to_type, to_id, to_hexagon, travel_method,
         distance_meters, duration_minutes, calculation_status, error_message, 
         last_calculated_at, created_at, updated_at)
        VALUES %s
    """
    # template matches the tuple structure in run()
    template = "(%s, %s, NULL, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    execute_values(cursor, sql, data, template=template)


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)