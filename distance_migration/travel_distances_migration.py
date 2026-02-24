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
from psycopg2 import OperationalError, InterfaceError

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

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
DB_INSERT_BATCH_SIZE = 10000

# Cache Directory for Resume capability
SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_DIR = SCRIPT_DIR / ".cache" / "travel_migration"
ENTITIES_CACHE_FILE = CACHE_DIR / "entities.json"


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


def get_existing_pairs(connection, from_type, to_type, method):
    """Return set of (from_id, to_id) already in DB for this segment (for smart retry insert skip)."""
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT from_id, to_id
            FROM travel_distances
            WHERE from_type = %s AND to_type = %s AND travel_method = %s
        """, (from_type, to_type, method))
        return {(row["from_id"], row["to_id"]) for row in cursor.fetchall()}
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
    execute_values(cursor, sql, data, template=template, page_size=5000)


# --- Cache Helpers (msgpack for speed, JSON fallback for backward compatibility) ---

try:
    import msgpack
except ImportError:
    msgpack = None


def _get_cache_base(method, from_type, to_type):
    """Base path for segment cache (no extension)."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{method}_{from_type}_{to_type}"


def _serialize_map(m):
    return {f"{a},{b}": v for (a, b), v in m.items()}


def _deserialize_map(j):
    if not j:
        return {}
    return {tuple(int(x) for x in k.split(",", 1)): v for k, v in j.items()}


def save_cache(method, from_type, to_type, distance, duration):
    base = _get_cache_base(method, from_type, to_type)
    if msgpack:
        path = base.with_suffix(".msgpack")
        logger.info(f"    Saving API results to cache: {path.name}")
        try:
            rows = []
            for (src_id, tgt_id), dist_km in distance.items():
                if dist_km is None:
                    continue
                dur_min = duration.get((src_id, tgt_id), 0)
                rows.append([src_id, tgt_id, dist_km, dur_min])
            path.write_bytes(msgpack.packb({"rows": rows}))
            logger.info(f"    Cache saved successfully ({path.stat().st_size} bytes)")
        except Exception as e:
            logger.error(f"    Failed to save cache: {e}")
    else:
        path = base.with_suffix(".json")
        logger.info(f"    Saving API results to cache: {path.name}")
        try:
            data = {
                "distance": _serialize_map(distance),
                "duration": _serialize_map(duration),
            }
            path.write_text(json.dumps(data), encoding="utf-8")
            logger.info(f"    Cache saved successfully ({path.stat().st_size} bytes)")
        except Exception as e:
            logger.error(f"    Failed to save cache: {e}")


def _load_cache_msgpack(path):
    """Load cache from msgpack format; returns None on failure."""
    if not path.exists() or not msgpack:
        return None
    try:
        raw = msgpack.unpackb(path.read_bytes(), strict_map_key=False)
        rows = raw.get("rows", [])
        distance = {}
        duration = {}
        for r in rows:
            src_id, tgt_id, dist_km, dur_min = r[0], r[1], r[2], r[3]
            key = (int(src_id), int(tgt_id))
            distance[key] = dist_km
            duration[key] = dur_min
        return {"distance": distance, "duration": duration}
    except Exception:
        return None


def _load_cache_json(path):
    """Load cache from legacy JSON format; returns None on failure."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {
            "distance": _deserialize_map(data.get("distance", {})),
            "duration": _deserialize_map(data.get("duration", {})),
        }
    except Exception:
        return None


def load_cache(method, from_type, to_type):
    base = _get_cache_base(method, from_type, to_type)
    path_mp = base.with_suffix(".msgpack")
    path_json = base.with_suffix(".json")
    if path_mp.exists():
        logger.info(f"    >>> CACHE FOUND: {path_mp.name} (Resuming from previous attempt)")
        matrix = _load_cache_msgpack(path_mp)
        if matrix:
            return matrix
    if path_json.exists():
        logger.info(f"    >>> CACHE FOUND: {path_json.name} (Resuming from previous attempt)")
        matrix = _load_cache_json(path_json)
        if matrix:
            return matrix
    logger.info(f"    Cache not found at {base.name}[.msgpack|.json]")
    return None


def clear_cache(method, from_type, to_type):
    base = _get_cache_base(method, from_type, to_type)
    cleared = []
    for ext in (".msgpack", ".json"):
        path = base.with_suffix(ext)
        if path.exists():
            try:
                path.unlink()
                cleared.append(path.name)
            except OSError:
                pass
    if cleared:
        logger.info(f"    Cleared cache file(s): {', '.join(cleared)}")


def _segment_key(osrm_method, from_type, to_type):
    return f"{osrm_method}_{from_type}_{to_type}"


def _load_entities_cache():
    """Load users and clients from local cache if present."""
    if not ENTITIES_CACHE_FILE.exists():
        return None, None
    try:
        data = json.loads(ENTITIES_CACHE_FILE.read_text(encoding="utf-8"))
        users = {int(k): v for k, v in data.get("users", {}).items()}
        clients = {int(k): v for k, v in data.get("clients", {}).items()}
        logger.info("Loaded entities from cache: %d users, %d clients", len(users), len(clients))
        return users, clients
    except Exception as e:
        logger.warning("Failed to load entities cache: %s", e)
        return None, None


def _save_entities_cache(users, clients):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data = {"users": users, "clients": clients}
    ENTITIES_CACHE_FILE.write_text(json.dumps(data), encoding="utf-8")
    logger.info("Saved entities cache: %d users, %d clients", len(users), len(clients))


def process_missing_segment(
    connection,
    cursor,
    source_entities,
    target_entities,
    missing_source_ids,
    from_type,
    to_type,
    osrm_method,
    db_enum_method,
    state=None,
    segment_key=None,
):
    if not missing_source_ids:
        logger.info(f"  {from_type}->{to_type} ({db_enum_method}): No missing pairs.")
        return
    logger.info(f"  {from_type}->{to_type} ({db_enum_method}): Found {len(missing_source_ids)} sources with missing data.")
    matrix = load_cache(osrm_method, from_type, to_type)
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
            step_size=DEFAULT_STEP_SIZE,
        )
        save_cache(osrm_method, from_type, to_type, matrix["distance"], matrix["duration"])
    else:
        logger.info("    Using cached data (Skipping OSRM API call).")
    # Smart retry: only insert pairs that are not already in DB (avoids millions of conflict checks)
    try:
        existing_pairs = get_existing_pairs(connection, from_type, to_type, db_enum_method)
    except (OperationalError, InterfaceError) as e:
        if ConnectionLostError:
            completed = state.get_step("distance_migration").get("completed_segments") or [] if state else []
            raise ConnectionLostError("distance_migration", dict(completed_segments=completed, current_segment=segment_key)) from e
        raise
    total_cached = len(matrix["distance"])
    now = datetime.utcnow()
    batch_data = []
    for (src_id, tgt_id), dist_km in matrix["distance"].items():
        if dist_km is None:
            continue
        if (src_id, tgt_id) in existing_pairs:
            continue
        dur_min = matrix["duration"].get((src_id, tgt_id), 0)
        batch_data.append((
            from_type, src_id, to_type, tgt_id, db_enum_method,
            int(round(dist_km * 1000)), int(dur_min) if dur_min else 0,
            CALCULATION_STATUS_COMPLETED, None, now, now, now,
        ))
    skipped = total_cached - len(batch_data)
    if skipped:
        logger.info("    Skipping %d pairs already in DB (inserting only %d missing).", skipped, len(batch_data))
    if not batch_data:
        logger.info("    No missing pairs to insert.")
        clear_cache(osrm_method, from_type, to_type)
        return
    total_batches = (len(batch_data) + DB_INSERT_BATCH_SIZE - 1) // DB_INSERT_BATCH_SIZE
    logger.info("    Inserting %d rows in %d batches (batch size %d)...", len(batch_data), total_batches, DB_INSERT_BATCH_SIZE)
    batches_committed = 0
    for i in range(0, len(batch_data), DB_INSERT_BATCH_SIZE):
        chunk = batch_data[i:i + DB_INSERT_BATCH_SIZE]
        try:
            insert_batch(cursor, chunk)
            connection.commit()
            batches_committed += 1
            if batches_committed % 10 == 0 or batches_committed == total_batches:
                logger.info("    Insert progress: %d / %d batches", batches_committed, total_batches)
            if state and segment_key:
                state.update("distance_migration", current_segment=segment_key)
        except (OperationalError, InterfaceError) as e:
            if ConnectionLostError:
                completed = state.get_step("distance_migration").get("completed_segments") or [] if state else []
                raise ConnectionLostError("distance_migration", dict(
                    completed_segments=completed,
                    current_segment=segment_key,
                )) from e
            raise
    logger.info("    Inserted/Updated records (batches committed: %d).", batches_committed)
    clear_cache(osrm_method, from_type, to_type)


def _build_segments_list():
    """List of (segment_key, from_type, to_type, osrm_method, db_enum_method) in execution order."""
    out = []
    for osrm_method in OSRM_METHODS:
        db_enum = TRAVEL_METHOD_MAP[osrm_method]
        out.append((_segment_key(osrm_method, ENTITY_TYPE_USER, ENTITY_TYPE_USER), ENTITY_TYPE_USER, ENTITY_TYPE_USER, osrm_method, db_enum))
        out.append((_segment_key(osrm_method, ENTITY_TYPE_CLIENT, ENTITY_TYPE_CLIENT), ENTITY_TYPE_CLIENT, ENTITY_TYPE_CLIENT, osrm_method, db_enum))
        out.append((_segment_key(osrm_method, ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT), ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT, osrm_method, db_enum))
        out.append((_segment_key(osrm_method, ENTITY_TYPE_CLIENT, ENTITY_TYPE_USER), ENTITY_TYPE_CLIENT, ENTITY_TYPE_USER, osrm_method, db_enum))
    return out


def run(connection_manager=None, state=None):
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     Travel Distances Migration (RESUME ENABLED)          ║
    ║     Find Missing -> Cache -> Insert                      ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    logger.info("Cache directory: %s", CACHE_DIR)
    if state and state.is_completed("distance_migration"):
        logger.info("Distance migration already completed (resume).")
        return True
    config = get_db_config()
    if not all([config["database"], config["user"], config["password"]]):
        logger.error("Missing DB_NAME, DB_USER, DB_PASSWORD")
        return False
    connection = None
    try:
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
            connection = connect_to_database(config)
        cursor = connection.cursor()
        completed_segments = list(state.get_step("distance_migration").get("completed_segments") or []) if state else []
        users = None
        clients = None
        if state and state.get("distance_migration", "entities_loaded"):
            users, clients = _load_entities_cache()
        if users is None or clients is None:
            try:
                users = load_users_with_locations(connection)
                clients = load_clients_with_locations(connection)
            except (OperationalError, InterfaceError) as e:
                if ConnectionLostError:
                    raise ConnectionLostError("distance_migration", dict(completed_segments=completed_segments)) from e
                raise
            _save_entities_cache(users, clients)
            if state:
                state.update("distance_migration", entities_loaded=True)
        user_ids = set(users.keys())
        client_ids = set(clients.keys())
        segments = _build_segments_list()
        for segment_key, from_type, to_type, osrm_method, db_enum_method in segments:
            if segment_key in completed_segments:
                logger.info("Skipping completed segment: %s", segment_key)
                continue
            source_entities = users if from_type == ENTITY_TYPE_USER else clients
            target_entities = users if to_type == ENTITY_TYPE_USER else clients
            if not source_entities or not target_entities:
                continue
            expected_count = len(target_entities)
            try:
                counts = get_existing_pair_counts(connection, from_type, to_type, db_enum_method)
            except (OperationalError, InterfaceError) as e:
                if ConnectionLostError:
                    raise ConnectionLostError("distance_migration", dict(completed_segments=completed_segments, current_segment=segment_key)) from e
                raise
            missing_src = find_missing_source_ids(
                user_ids if from_type == ENTITY_TYPE_USER else client_ids, counts, expected_count
            )
            process_missing_segment(
                connection, cursor,
                source_entities=source_entities,
                target_entities=target_entities,
                missing_source_ids=missing_src,
                from_type=from_type,
                to_type=to_type,
                osrm_method=osrm_method,
                db_enum_method=db_enum_method,
                state=state,
                segment_key=segment_key,
            )
            if state:
                completed_segments = list(completed_segments) + [segment_key]
                state.update("distance_migration", completed_segments=completed_segments, current_segment=segment_key)
        logger.info("")
        logger.info("Migration completed successfully.")
        try:
            cursor.execute("SELECT COUNT(*) AS cnt FROM travel_distances")
            total_rows = cursor.fetchone()["cnt"]
            logger.info("Total rows in table: %s", total_rows)
        except (OperationalError, InterfaceError) as e:
            if ConnectionLostError:
                raise ConnectionLostError("distance_migration", dict(completed_segments=completed_segments)) from e
            raise
        if state:
            state.clear_step("distance_migration")
            if ENTITIES_CACHE_FILE.exists():
                try:
                    ENTITIES_CACHE_FILE.unlink()
                except OSError:
                    pass
        return True
    except (OperationalError, InterfaceError):
        if connection and connection.closed == 0:
            try:
                connection.rollback()
            except Exception:
                pass
        raise
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
        if connection and not connection_manager and connection.closed == 0:
            connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)