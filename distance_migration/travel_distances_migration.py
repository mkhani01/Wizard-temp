"""
Travel Distances Migration (Smart Incremental with Resume Support)
1. Finds missing pairs from DB.
2. Checks for local cache files (allows retry without re-calling API).
3. Calculates missing pairs via OSRM (if not cached).
4. Bulk inserts results.
5. Deletes cache only on success.
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import OperationalError

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

# Batch sizes
DEFAULT_STEP_SIZE = 25
DB_INSERT_BATCH_SIZE = 1000

# Cache Directory for Resume capability
# Use absolute path based on script location
SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_DIR = SCRIPT_DIR / ".cache" / "travel_migration"


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
            connect_timeout=10,
            keepalives=1, 
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
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


def get_existing_pair_counts(connection, from_type, to_type, method):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT from_id, COUNT(*) as cnt
            FROM travel_distances
            WHERE from_type = %s AND to_type = %s AND travel_method = %s
            GROUP BY from_id
        """, (from_type, to_type, method))
        
        return {row['from_id']: row['cnt'] for row in cursor.fetchall()}
    finally:
        cursor.close()


def find_missing_source_ids(all_source_ids, existing_counts, expected_target_count):
    missing_ids = set()
    for sid in all_source_ids:
        cnt = existing_counts.get(sid, 0)
        if cnt < expected_target_count:
            missing_ids.add(sid)
    return missing_ids


def insert_batch(cursor, data):
    if not data:
        return
    sql = """
        INSERT INTO travel_distances
        (from_type, from_id, from_hexagon, to_type, to_id, to_hexagon, travel_method,
         distance_meters, duration_minutes, calculation_status, error_message, 
         last_calculated_at, created_at, updated_at)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    template = "(%s, %s, NULL, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)"
    execute_values(cursor, sql, data, template=template)


# --- Cache Helpers ---

def _get_cache_path(method, from_type, to_type):
    """Generate a unique filename for a specific segment."""
    # Ensure directory exists
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{method}_{from_type}_{to_type}.json"

def _serialize_map(m):
    return {f"{a},{b}": v for (a, b), v in m.items()}

def _deserialize_map(j):
    if not j:
        return {}
    return {tuple(int(x) for x in k.split(",", 1)): v for k, v in j.items()}

def save_cache(method, from_type, to_type, distance, duration):
    path = _get_cache_path(method, from_type, to_type)
    logger.info(f"    Saving API results to cache: {path.name}")
    try:
        data = {
            "distance": _serialize_map(distance),
            "duration": _serialize_map(duration)
        }
        path.write_text(json.dumps(data), encoding="utf-8")
        # Verify file exists immediately
        if not path.exists():
            logger.error(f"    CRITICAL: Saved cache but file does not exist at {path}")
        else:
            logger.info(f"    Cache saved successfully ({path.stat().st_size} bytes)")
    except Exception as e:
        logger.error(f"    Failed to save cache: {e}")

def load_cache(method, from_type, to_type):
    path = _get_cache_path(method, from_type, to_type)
    if path.exists():
        logger.info(f"    >>> CACHE FOUND: {path.name} (Resuming from previous attempt)")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return {
                "distance": _deserialize_map(data.get("distance", {})),
                "duration": _deserialize_map(data.get("duration", {}))
            }
        except Exception as e:
            logger.warning(f"    Failed to read cache: {e}")
    else:
        logger.info(f"    Cache not found at {path.name}")
    return None

def clear_cache(method, from_type, to_type):
    path = _get_cache_path(method, from_type, to_type)
    if path.exists():
        try:
            path.unlink()
            logger.info(f"    Cleared cache file: {path.name}")
        except OSError:
            pass


def process_missing_segment(
    connection, 
    cursor,
    source_entities, 
    target_entities, 
    missing_source_ids,
    from_type, 
    to_type, 
    osrm_method, 
    db_enum_method
):
    if not missing_source_ids:
        logger.info(f"  {from_type}→{to_type} ({db_enum_method}): No missing pairs.")
        return

    logger.info(f"  {from_type}→{to_type} ({db_enum_method}): Found {len(missing_source_ids)} sources with missing data.")
    
    # 1. Check for existing cache
    matrix = load_cache(osrm_method, from_type, to_type)
    
    # 2. If no cache, call API
    if not matrix:
        sources_to_calc = {sid: source_entities[sid] for sid in missing_source_ids if sid in source_entities}
        
        if not sources_to_calc:
            logger.warning("    No coordinate data found for missing source IDs.")
            return

        logger.info(f"    >>> CALLING OSRM API: {len(sources_to_calc)} x {len(target_entities)}")
        
        matrix = get_distance_matrix(
            entities_info1=sources_to_calc,
            entities_info2=target_entities,
            travel_method=osrm_method,
            step_size=DEFAULT_STEP_SIZE
        )
        
        # Save to cache immediately
        save_cache(osrm_method, from_type, to_type, matrix['distance'], matrix['duration'])
    else:
        logger.info(f"    Using cached data (Skipping OSRM API call).")

    # 3. Prepare Insert Data
    batch_data = []
    now = datetime.utcnow()
    
    for (src_id, tgt_id), dist_km in matrix['distance'].items():
        if dist_km is None:
            continue
        
        dur_min = matrix['duration'].get((src_id, tgt_id), 0)
        
        batch_data.append((
            from_type,
            src_id,
            to_type,
            tgt_id,
            db_enum_method,
            int(round(dist_km * 1000)),
            int(dur_min) if dur_min else 0,
            CALCULATION_STATUS_COMPLETED,
            None,
            now,
            now,
            now,
        ))

        if len(batch_data) >= DB_INSERT_BATCH_SIZE:
            try:
                insert_batch(cursor, batch_data)
                connection.commit()
                batch_data = []
            except Exception as e:
                logger.error(f"    Database insert error: {e}")
                raise

    if batch_data:
        try:
            insert_batch(cursor, batch_data)
            connection.commit()
        except Exception as e:
            logger.error(f"    Database insert error: {e}")
            raise

    logger.info(f"    ✓ Inserted/Updated {len(batch_data)} records.")
    
    # 4. Clear cache ONLY after successful DB commit
    clear_cache(osrm_method, from_type, to_type)


def run():
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     Travel Distances Migration (RESUME ENABLED)          ║
    ║     Find Missing -> Cache -> Insert                      ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Log the cache directory being used
    logger.info(f"Cache directory: {CACHE_DIR}")

    config = get_db_config()
    if not all([config["database"], config["user"], config["password"]]):
        logger.error("Missing DB_NAME, DB_USER, DB_PASSWORD")
        return False

    connection = None
    try:
        connection = connect_to_database(config)
        
        # 1. Load Entities
        users = load_users_with_locations(connection)
        clients = load_clients_with_locations(connection)
        user_ids = set(users.keys())
        client_ids = set(clients.keys())

        cursor = connection.cursor()

        # 2. Process by Travel Method
        for osrm_method in OSRM_METHODS:
            db_enum_method = TRAVEL_METHOD_MAP[osrm_method]
            logger.info("")
            logger.info("=" * 60)
            logger.info(f"Travel method: {osrm_method} (saving as '{db_enum_method}')")
            logger.info("=" * 60)

            # --- User -> User ---
            if users:
                logger.info(f"Checking User→User...")
                counts = get_existing_pair_counts(connection, ENTITY_TYPE_USER, ENTITY_TYPE_USER, db_enum_method)
                missing_src = find_missing_source_ids(user_ids, counts, len(user_ids))
                
                process_missing_segment(
                    connection, cursor,
                    source_entities=users,
                    target_entities=users,
                    missing_source_ids=missing_src,
                    from_type=ENTITY_TYPE_USER,
                    to_type=ENTITY_TYPE_USER,
                    osrm_method=osrm_method,
                    db_enum_method=db_enum_method
                )

            # --- Client -> Client ---
            if clients:
                logger.info(f"Checking Client→Client...")
                counts = get_existing_pair_counts(connection, ENTITY_TYPE_CLIENT, ENTITY_TYPE_CLIENT, db_enum_method)
                missing_src = find_missing_source_ids(client_ids, counts, len(client_ids))

                process_missing_segment(
                    connection, cursor,
                    source_entities=clients,
                    target_entities=clients,
                    missing_source_ids=missing_src,
                    from_type=ENTITY_TYPE_CLIENT,
                    to_type=ENTITY_TYPE_CLIENT,
                    osrm_method=osrm_method,
                    db_enum_method=db_enum_method
                )

            # --- User -> Client ---
            if users and clients:
                logger.info(f"Checking User→Client...")
                counts = get_existing_pair_counts(connection, ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT, db_enum_method)
                missing_src = find_missing_source_ids(user_ids, counts, len(client_ids))

                process_missing_segment(
                    connection, cursor,
                    source_entities=users,
                    target_entities=clients,
                    missing_source_ids=missing_src,
                    from_type=ENTITY_TYPE_USER,
                    to_type=ENTITY_TYPE_CLIENT,
                    osrm_method=osrm_method,
                    db_enum_method=db_enum_method
                )

            # --- Client -> User ---
            if users and clients:
                logger.info(f"Checking Client→User...")
                counts = get_existing_pair_counts(connection, ENTITY_TYPE_CLIENT, ENTITY_TYPE_USER, db_enum_method)
                missing_src = find_missing_source_ids(client_ids, counts, len(user_ids))

                process_missing_segment(
                    connection, cursor,
                    source_entities=clients,
                    target_entities=users,
                    missing_source_ids=missing_src,
                    from_type=ENTITY_TYPE_CLIENT,
                    to_type=ENTITY_TYPE_USER,
                    osrm_method=osrm_method,
                    db_enum_method=db_enum_method
                )

        logger.info("")
        logger.info("✓ Migration completed successfully.")
        
        # Verify
        cursor.execute("SELECT COUNT(*) AS cnt FROM travel_distances")
        total_rows = cursor.fetchone()["cnt"]
        logger.info(f"Total rows in table: {total_rows}")
        
        return True
        
    except Exception as e:
        if connection and connection.closed == 0:
            try:
                connection.rollback()
            except Exception:
                pass
        
        logger.exception("Migration failed: %s", e)
        logger.error("IMPORTANT: You can retry the script. Cached API results will be reused.")
        return False
    finally:
        if connection and connection.closed == 0:
            connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)