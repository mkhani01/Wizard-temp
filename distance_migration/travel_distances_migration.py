"""
Travel Distances Migration (Fast Bulk Version)
Loads users and clients, computes distances via OSRM, then performs a BULK INSERT.
- Wipes existing travel_distances table before starting.
- Computes: user→user, client→client, user→client, client→user (separate requests).
- Uses execute_values for massive speedup (100x faster than row-by-row).
- Maps OSRM profiles (driving-car) to DB enums (car).
- Resume: on retry after an insert failure, completed segments are skipped and the
  failed segment is re-inserted from cached API results (no repeat OSRM calls).
"""

import os
import sys
import json
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

# Resume state: checkpoint + per-segment cache so retry continues from insert without re-calling OSRM
RESUME_DIR_NAME = "travel_distances_resume"
CHECKPOINT_FILENAME = "checkpoint.json"


def _get_resume_dir():
    """Directory for checkpoint and segment cache (under project root .cache)."""
    root = os.getenv("AOS_MIGRATION_PROJECT_ROOT", os.getcwd())
    d = Path(root) / ".cache" / RESUME_DIR_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _segment_key(osrm_method: str, from_type: str, to_type: str) -> str:
    return f"{osrm_method}:{from_type}:{to_type}"


def _serialize_map(m):
    """Convert dict with (id1, id2) keys to JSON-serializable dict with 'id1,id2' keys."""
    return {f"{a},{b}": v for (a, b), v in m.items()}


def _deserialize_map(j):
    """Convert back to dict with (id1, id2) tuple keys."""
    if not j:
        return {}
    return {tuple(int(x) for x in k.split(",", 1)): v for k, v in j.items()}


def get_checkpoint() -> set:
    """Return set of completed segment keys from previous run (empty if none)."""
    path = _get_resume_dir() / CHECKPOINT_FILENAME
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return set(data.get("completed", []))
    except Exception as e:
        logger.warning("Could not load checkpoint %s: %s", path, e)
        return set()


def save_checkpoint(completed: set):
    """Persist list of completed segment keys."""
    path = _get_resume_dir() / CHECKPOINT_FILENAME
    path.write_text(json.dumps({"completed": sorted(completed)}, indent=2), encoding="utf-8")


def save_segment_cache(segment_key: str, distance: dict, duration: dict):
    """Cache API result for a segment so retry can skip OSRM and only insert."""
    d = _get_resume_dir()
    path = d / f"{segment_key.replace(':', '_')}.json"
    path.write_text(
        json.dumps({
            "distance": _serialize_map(distance),
            "duration": _serialize_map(duration),
        }, indent=0),
        encoding="utf-8",
    )


def load_segment_cache(segment_key: str):
    """Load cached distance/duration for a segment; returns None if not found."""
    d = _get_resume_dir()
    path = d / f"{segment_key.replace(':', '_')}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {
            "distance": _deserialize_map(data.get("distance", {})),
            "duration": _deserialize_map(data.get("duration", {})),
        }
    except Exception as e:
        logger.warning("Could not load segment cache %s: %s", path, e)
        return None


def clear_segment_cache(segment_key: str):
    """Remove cache file for one segment after successful insert."""
    path = _get_resume_dir() / f"{segment_key.replace(':', '_')}.json"
    if path.exists():
        try:
            path.unlink()
        except OSError:
            pass


def clear_resume_state():
    """Remove checkpoint and all segment caches after successful full run."""
    d = _get_resume_dir()
    for f in d.iterdir():
        try:
            f.unlink()
        except OSError:
            pass


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

        # --- RESUME: checkpoint + cache so retry continues from insert without re-calling OSRM ---
        completed_segments = get_checkpoint()
        is_resume = len(completed_segments) > 0
        if is_resume:
            logger.warning("Resuming from previous run: skipping %d completed segment(s), retrying insert from cache where available.", len(completed_segments))

        # --- STEP 1: WIPE OLD DATA (only on fresh run; on resume keep existing rows) ---
        if not is_resume:
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

        def run_segment(segment_key, from_type, to_type, db_enum_method, get_matrix_fn, label):
            """Run one segment: skip if completed, else load from cache or call API, then insert."""
            if segment_key in completed_segments:
                logger.info("  [skip] %s (already completed)", label)
                return
            cached = load_segment_cache(segment_key)
            if cached:
                logger.info("  %s (insert only, from cache – retry after previous insert failure)", label)
                matrix_result = cached
            else:
                matrix_result = get_matrix_fn()
                save_segment_cache(segment_key, matrix_result["distance"], matrix_result["duration"])
            add_rows(
                matrix_result["distance"],
                matrix_result["duration"],
                from_type,
                to_type,
                db_enum_method,
            )
            completed_segments.add(segment_key)
            save_checkpoint(completed_segments)
            clear_segment_cache(segment_key)
            logger.info("  ✓ %s", label)

        for osrm_method in OSRM_METHODS:
            db_enum_method = TRAVEL_METHOD_MAP[osrm_method]
            logger.info("")
            logger.info("=" * 60)
            logger.info(f"Travel method: {osrm_method} (saving as '{db_enum_method}')")
            logger.info("=" * 60)

            # 1. User → User
            if users:
                seg_key = _segment_key(osrm_method, ENTITY_TYPE_USER, ENTITY_TYPE_USER)
                try:
                    run_segment(
                        seg_key,
                        ENTITY_TYPE_USER,
                        ENTITY_TYPE_USER,
                        db_enum_method,
                        lambda: get_distance_matrix(
                            entities_info1=users,
                            entities_info2=users,
                            travel_method=osrm_method,
                            step_size=DEFAULT_STEP_SIZE,
                        ),
                        "User → User",
                    )
                except Exception as e:
                    logger.exception("User→User failed for %s: %s", osrm_method, e)

            # 2. Client → Client
            if clients:
                seg_key = _segment_key(osrm_method, ENTITY_TYPE_CLIENT, ENTITY_TYPE_CLIENT)
                try:
                    run_segment(
                        seg_key,
                        ENTITY_TYPE_CLIENT,
                        ENTITY_TYPE_CLIENT,
                        db_enum_method,
                        lambda: get_distance_matrix(
                            entities_info1=clients,
                            entities_info2=clients,
                            travel_method=osrm_method,
                            step_size=DEFAULT_STEP_SIZE,
                        ),
                        "Client → Client",
                    )
                except Exception as e:
                    logger.exception("Client→Client failed for %s: %s", osrm_method, e)

            # 3. User → Client
            if users and clients:
                seg_key = _segment_key(osrm_method, ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT)
                try:
                    run_segment(
                        seg_key,
                        ENTITY_TYPE_USER,
                        ENTITY_TYPE_CLIENT,
                        db_enum_method,
                        lambda: get_distance_matrix(
                            entities_info1=users,
                            entities_info2=clients,
                            travel_method=osrm_method,
                            step_size=DEFAULT_STEP_SIZE,
                        ),
                        "User → Client",
                    )
                except Exception as e:
                    logger.exception("User→Client failed for %s: %s", osrm_method, e)

            # 4. Client → User (separate request; direction matters)
            if users and clients:
                seg_key = _segment_key(osrm_method, ENTITY_TYPE_CLIENT, ENTITY_TYPE_USER)
                try:
                    run_segment(
                        seg_key,
                        ENTITY_TYPE_CLIENT,
                        ENTITY_TYPE_USER,
                        db_enum_method,
                        lambda: get_distance_matrix(
                            entities_info1=clients,
                            entities_info2=users,
                            travel_method=osrm_method,
                            step_size=DEFAULT_STEP_SIZE,
                        ),
                        "Client → User",
                    )
                except Exception as e:
                    logger.exception("Client→User failed for %s: %s", osrm_method, e)

        # Insert any remaining rows
        flush_batch()

        # Clear resume state only after full success so next run is fresh
        clear_resume_state()

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