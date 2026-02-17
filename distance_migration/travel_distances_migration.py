"""
Travel Distances Migration (Fast Bulk Version)
Loads users and clients, computes distances via OSRM, then performs a BULK INSERT.
- Wipes existing travel_distances table before starting.
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

        if not users or not clients:
            logger.warning("No users with coordinates or no clients with coordinates. Nothing to migrate.")
            return True

        cursor = connection.cursor()
        
        # --- STEP 1: WIPE OLD DATA ---
        logger.warning("Clearing existing travel_distances table...")
        cursor.execute("TRUNCATE TABLE travel_distances RESTART IDENTITY;")
        connection.commit()
        logger.info("✓ Table cleared.")
        
        total_expected = len(users) * len(clients) * len(OSRM_METHODS)
        logger.info(f"Total expected distance records: {total_expected}")

        # --- STEP 2: CALCULATE & BULK INSERT ---
        for osrm_method in OSRM_METHODS:
            db_enum_method = TRAVEL_METHOD_MAP[osrm_method]
            logger.info("")
            logger.info("=" * 60)
            logger.info(f"Travel method: {osrm_method} (saving as '{db_enum_method}')")
            logger.info("=" * 60)
            
            try:
                matrix_result = get_distance_matrix(
                    entities_info1=users,
                    entities_info2=clients,
                    travel_method=osrm_method,
                    step_size=DEFAULT_STEP_SIZE,
                )
            except Exception as e:
                logger.exception("OSRM request failed for %s", osrm_method)
                # Continue to next method instead of crashing everything
                continue

            dist_map = matrix_result["distance"]
            dur_map = matrix_result["duration"]

            # Prepare batch data
            now = datetime.utcnow()
            batch_data = []
            processed_count = 0

            for (user_id, client_id), dist_km in dist_map.items():
                if dist_km is None:
                    continue
                
                dur_min = dur_map.get((user_id, client_id), 0)
                distance_meters = int(round(dist_km * 1000))
                duration_minutes = int(dur_min) if dur_min else 0
                
                # Create a tuple for the insert
                # Columns: from_type, from_id, to_type, to_id, travel_method, distance_meters, duration_minutes, status, error, last_calc, created, updated
                row_data = (
                    ENTITY_TYPE_USER,
                    user_id,
                    ENTITY_TYPE_CLIENT,
                    client_id,
                    db_enum_method,
                    distance_meters,
                    duration_minutes,
                    CALCULATION_STATUS_COMPLETED,
                    None, # error_message
                    now,  # last_calculated_at
                    now,  # created_at
                    now   # updated_at
                )
                batch_data.append(row_data)
                processed_count += 1

                # Insert in chunks of DB_INSERT_BATCH_SIZE
                if len(batch_data) >= DB_INSERT_BATCH_SIZE:
                    insert_batch(cursor, batch_data)
                    connection.commit()
                    logger.info(f"  Inserted {processed_count} / {len(dist_map)} records for {db_enum_method}...")
                    batch_data = [] # Reset batch

            # Insert remaining items for this method
            if batch_data:
                insert_batch(cursor, batch_data)
                connection.commit()
                logger.info(f"  Inserted {processed_count} / {len(dist_map)} records for {db_enum_method}...")

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