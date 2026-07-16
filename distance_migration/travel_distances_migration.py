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
import io
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import OperationalError, InterfaceError

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

try:
    from distance_migration.pair_scope import (
        build_required_pairs,
        build_full_matrix_pairs,
        get_distance_mode,
        resolve_visit_csv_path,
    )
except ImportError:
    build_required_pairs = None
    build_full_matrix_pairs = None
    get_distance_mode = lambda: os.getenv("DISTANCE_MODE", "full").strip().lower()
    resolve_visit_csv_path = lambda explicit_path=None: None

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
DB_INSERT_BATCH_SIZE = int(os.getenv("DB_INSERT_BATCH_SIZE", "5000"))
PIPELINE_FLUSH_PAIRS = int(os.getenv("PIPELINE_FLUSH_PAIRS", "250"))

# Cache Directory for Resume capability
SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_DIR = SCRIPT_DIR / ".cache" / "travel_migration"
ENTITIES_CACHE_FILE = CACHE_DIR / "entities.json"

# Cached enum ::cast suffixes for travel_distances text → enum inserts.
_TD_ENUM_CASTS_CACHE = None


def _resolve_travel_distances_enum_casts(cursor):
    """Map travel_distances enum columns to ::type casts for text staging inserts."""
    global _TD_ENUM_CASTS_CACHE
    if _TD_ENUM_CASTS_CACHE is not None:
        return _TD_ENUM_CASTS_CACHE

    enum_columns = (
        "from_type",
        "to_type",
        "travel_method",
        "calculation_status",
    )
    casts = {col: "" for col in enum_columns}
    used_fallback = False
    try:
        cursor.execute("""
            SELECT a.attname, t.typname
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
            WHERE c.relname = 'travel_distances'
              AND n.nspname = current_schema()
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND t.typcategory = 'E'
              AND a.attname = ANY(%s)
        """, (list(enum_columns),))
        rows = cursor.fetchall()
        if not rows:
            used_fallback = True
        else:
            for row in rows:
                casts[row["attname"]] = f"::{row['typname']}"
    except Exception as e:
        logger.debug("Could not resolve travel_distances enum casts from catalog: %s", e)
        used_fallback = True

    if used_fallback:
        casts = {
            "from_type": "::travel_distances_from_type_enum",
            "to_type": "::travel_distances_to_type_enum",
            "travel_method": "::travel_distances_travel_method_enum",
            "calculation_status": "::travel_distances_calculation_status_enum",
        }

    _TD_ENUM_CASTS_CACHE = casts
    return casts


def _stage_insert_select_sql(casts):
    return (
        f"from_type{casts['from_type']}, from_id, NULL, "
        f"to_type{casts['to_type']}, to_id, NULL, travel_method{casts['travel_method']}, "
        f"distance_meters, duration_minutes, calculation_status{casts['calculation_status']}, "
        f"error_message, last_calculated_at, created_at, updated_at"
    )


def _insert_values_template(casts):
    return (
        f"(%s{casts['from_type']}, %s, NULL, %s{casts['to_type']}, %s, NULL, "
        f"%s{casts['travel_method']}, %s, %s, %s{casts['calculation_status']}, "
        f"%s, %s, %s, %s)"
    )


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


BULK_PAIR_LOOKUP_THRESHOLD = int(os.getenv("BULK_PAIR_LOOKUP_THRESHOLD", "10000"))
EXISTING_PAIRS_FETCH_BATCH = int(os.getenv("EXISTING_PAIRS_FETCH_BATCH", "100000"))


def get_segment_row_count(connection, from_type, to_type, method):
    """Fast COUNT(*) for a travel_distances segment."""
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) AS cnt
            FROM travel_distances
            WHERE from_type = %s AND to_type = %s AND travel_method = %s
        """, (from_type, to_type, method))
        return int(cursor.fetchone()["cnt"])
    finally:
        cursor.close()


def load_existing_pairs_for_segment(connection, from_type, to_type, method):
    """Load all (from_id, to_id) pairs for a segment in one indexed scan."""
    cursor = connection.cursor()
    try:
        logger.info(
            "    Loading existing %s->%s (%s) pairs from DB...",
            from_type, to_type, method,
        )
        cursor.execute("""
            SELECT from_id, to_id
            FROM travel_distances
            WHERE from_type = %s AND to_type = %s AND travel_method = %s
        """, (from_type, to_type, method))
        existing = set()
        fetched = 0
        while True:
            rows = cursor.fetchmany(EXISTING_PAIRS_FETCH_BATCH)
            if not rows:
                break
            existing.update((row["from_id"], row["to_id"]) for row in rows)
            fetched += len(rows)
            if fetched % EXISTING_PAIRS_FETCH_BATCH == 0:
                logger.info("    ... loaded %d existing pair(s) so far", fetched)
        logger.info("    Loaded %d existing pair(s) from DB", len(existing))
        return existing
    finally:
        cursor.close()


def get_existing_pairs_for_keys(connection, from_type, to_type, method, pair_keys):
    """Return subset of pair_keys already present in DB."""
    if not pair_keys:
        return set()
    if len(pair_keys) >= BULK_PAIR_LOOKUP_THRESHOLD:
        return pair_keys & load_existing_pairs_for_segment(
            connection, from_type, to_type, method,
        )
    cursor = connection.cursor()
    try:
        keys_list = list(pair_keys)
        existing = set()
        chunk = 5000
        for i in range(0, len(keys_list), chunk):
            sub = keys_list[i:i + chunk]
            from_ids = [p[0] for p in sub]
            to_ids = [p[1] for p in sub]
            cursor.execute("""
                SELECT from_id, to_id
                FROM travel_distances
                WHERE from_type = %s AND to_type = %s AND travel_method = %s
                  AND (from_id, to_id) IN (
                    SELECT * FROM UNNEST(%s::bigint[], %s::bigint[]) AS t(from_id, to_id)
                  )
            """, (from_type, to_type, method, from_ids, to_ids))
            for row in cursor.fetchall():
                existing.add((row["from_id"], row["to_id"]))
        return existing
    finally:
        cursor.close()


class PipelineInserter:
    """Buffer rows and flush via COPY + INSERT. DB work runs on a dedicated thread."""

    def __init__(
        self,
        connection,
        cursor,
        from_type,
        to_type,
        db_enum_method,
        state=None,
        segment_key=None,
        skip_conflict_check=False,
        async_insert=True,
    ):
        self.connection = connection
        self.cursor = cursor
        self.from_type = from_type
        self.to_type = to_type
        self.db_enum_method = db_enum_method
        self.state = state
        self.segment_key = segment_key
        self.skip_conflict_check = skip_conflict_check
        self.now = datetime.utcnow()
        self.buffer = []
        self.attempted_rows = 0
        self.batches_committed = 0
        self.processed_pairs = 0
        self._lock = threading.Lock()
        self._async = async_insert
        self._insert_pool = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="td-insert")
            if async_insert else None
        )
        self._pending = []

    def add_block(self, matrix):
        """Queue block for insert (async) or insert immediately (sync)."""
        if self._async and self._insert_pool is not None:
            fut = self._insert_pool.submit(self._add_block_locked, matrix)
            self._pending.append(fut)
        else:
            self._add_block_locked(matrix)

    def _add_block_locked(self, matrix):
        with self._lock:
            for (src_id, tgt_id), dist_km in matrix.get("distance", {}).items():
                self.processed_pairs += 1
                if dist_km is None:
                    continue
                dur_min = matrix.get("duration", {}).get((src_id, tgt_id), 0)
                self.buffer.append((
                    self.from_type, src_id, self.to_type, tgt_id, self.db_enum_method,
                    int(round(dist_km * 1000)), int(dur_min) if dur_min else 0,
                    CALCULATION_STATUS_COMPLETED, None, self.now, self.now, self.now,
                ))
                if len(self.buffer) >= PIPELINE_FLUSH_PAIRS:
                    self._flush_locked()
            if self.buffer:
                self._flush_locked()

    def flush(self):
        with self._lock:
            self._flush_locked()

    def _flush_locked(self):
        if not self.buffer:
            return
        try:
            _copy_insert_batch(
                self.cursor,
                self.buffer,
                skip_conflict_check=self.skip_conflict_check,
            )
            self.connection.commit()
            self.attempted_rows += len(self.buffer)
            self.batches_committed += 1
            logger.info(
                "    Insert progress: attempted %d row(s), committed %d batch(es)",
                self.attempted_rows,
                self.batches_committed,
            )
            if self.state and self.segment_key:
                self.state.update(
                    "distance_migration",
                    current_segment=self.segment_key,
                    inserted_rows=self.attempted_rows,
                )
            self.buffer = []
        except (OperationalError, InterfaceError) as e:
            self.connection.rollback()
            if ConnectionLostError:
                completed = self.state.get_step("distance_migration").get("completed_segments") or [] if self.state else []
                raise ConnectionLostError("distance_migration", dict(
                    completed_segments=completed,
                    current_segment=self.segment_key,
                )) from e
            raise

    def finish(self):
        if self._insert_pool is not None:
            for fut in self._pending:
                try:
                    fut.result()
                except Exception:
                    self._insert_pool.shutdown(wait=False)
                    raise
            self._insert_pool.shutdown(wait=True)
        self.flush()
        return self.attempted_rows, self.batches_committed


def _copy_insert_batch(cursor, rows, skip_conflict_check=False):
    """Insert rows using COPY into a temp table, then INSERT into travel_distances."""
    if not rows:
        return
    casts = _resolve_travel_distances_enum_casts(cursor)
    select_cols = _stage_insert_select_sql(casts)
    cursor.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _td_stage (
            from_type text,
            from_id bigint,
            to_type text,
            to_id bigint,
            travel_method text,
            distance_meters int,
            duration_minutes int,
            calculation_status text,
            error_message text,
            last_calculated_at timestamp,
            created_at timestamp,
            updated_at timestamp
        ) ON COMMIT DELETE ROWS
    """)
    cursor.execute("TRUNCATE _td_stage")

    buf = io.StringIO()
    for row in rows:
        parts = []
        for val in row:
            if val is None:
                parts.append("\\N")
            elif isinstance(val, datetime):
                parts.append(val.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                s = str(val).replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")
                parts.append(s)
        buf.write("\t".join(parts) + "\n")
    buf.seek(0)

    cursor.copy_expert("""
        COPY _td_stage (
            from_type, from_id, to_type, to_id, travel_method,
            distance_meters, duration_minutes, calculation_status, error_message,
            last_calculated_at, created_at, updated_at
        ) FROM STDIN WITH (FORMAT text, NULL '\\N')
    """, buf)

    if skip_conflict_check:
        cursor.execute(f"""
            INSERT INTO travel_distances (
                from_type, from_id, from_hexagon, to_type, to_id, to_hexagon, travel_method,
                distance_meters, duration_minutes, calculation_status, error_message,
                last_calculated_at, created_at, updated_at
            )
            SELECT
                {select_cols}
            FROM _td_stage
        """)
    else:
        cursor.execute(f"""
            INSERT INTO travel_distances (
                from_type, from_id, from_hexagon, to_type, to_id, to_hexagon, travel_method,
                distance_meters, duration_minutes, calculation_status, error_message,
                last_calculated_at, created_at, updated_at
            )
            SELECT
                {select_cols}
            FROM _td_stage
            ON CONFLICT DO NOTHING
        """)


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
    casts = _resolve_travel_distances_enum_casts(cursor)
    sql = """
        INSERT INTO travel_distances
        (from_type, from_id, from_hexagon, to_type, to_id, to_hexagon, travel_method,
         distance_meters, duration_minutes, calculation_status, error_message, 
         last_calculated_at, created_at, updated_at)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    template = _insert_values_template(casts)
    execute_values(cursor, sql, data, template=template, page_size=5000)


def insert_matrix_streaming(
    connection,
    cursor,
    matrix,
    from_type,
    to_type,
    db_enum_method,
    state=None,
    segment_key=None,
):
    """
    Stream calculated matrix rows into travel_distances in DB-sized chunks.
    This avoids loading all existing pairs and building a multi-million-row
    in-memory insert list before the first commit.
    """
    total_pairs = len(matrix["distance"])
    logger.info(
        "    Streaming %d calculated pair(s) into DB in chunks of %d...",
        total_pairs,
        DB_INSERT_BATCH_SIZE,
    )

    now = datetime.utcnow()
    chunk = []
    processed_pairs = 0
    attempted_rows = 0
    skipped_null = 0
    batches_committed = 0

    def commit_chunk():
        nonlocal chunk, attempted_rows, batches_committed
        if not chunk:
            return
        try:
            insert_batch(cursor, chunk)
            connection.commit()
            attempted_rows += len(chunk)
            batches_committed += 1
            logger.info(
                "    Insert progress: processed %d / %d pair(s), attempted %d row(s), committed %d batch(es)",
                processed_pairs,
                total_pairs,
                attempted_rows,
                batches_committed,
            )
            if state and segment_key:
                state.update("distance_migration", current_segment=segment_key)
            chunk = []
        except (OperationalError, InterfaceError) as e:
            connection.rollback()
            if ConnectionLostError:
                completed = state.get_step("distance_migration").get("completed_segments") or [] if state else []
                raise ConnectionLostError("distance_migration", dict(
                    completed_segments=completed,
                    current_segment=segment_key,
                )) from e
            raise

    for (src_id, tgt_id), dist_km in matrix["distance"].items():
        processed_pairs += 1
        if dist_km is None:
            skipped_null += 1
            continue

        dur_min = matrix["duration"].get((src_id, tgt_id), 0)
        chunk.append((
            from_type, src_id, to_type, tgt_id, db_enum_method,
            int(round(dist_km * 1000)), int(dur_min) if dur_min else 0,
            CALCULATION_STATUS_COMPLETED, None, now, now, now,
        ))

        if len(chunk) >= DB_INSERT_BATCH_SIZE:
            commit_chunk()

    commit_chunk()
    logger.info(
        "    Finished DB streaming: processed %d pair(s), attempted %d row(s), skipped %d null distance(s), committed %d batch(es)",
        processed_pairs,
        attempted_rows,
        skipped_null,
        batches_committed,
    )
    return attempted_rows, batches_committed


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


def _get_missing_pairs(connection, from_type, to_type, method, required_pairs):
    if not required_pairs:
        return set()
    db_count = get_segment_row_count(connection, from_type, to_type, method)
    if db_count == 0:
        return set(required_pairs)
    if db_count < len(required_pairs):
        logger.info(
            "    Segment has %d/%d required row(s); computing missing pairs...",
            db_count,
            len(required_pairs),
        )
    return required_pairs - get_existing_pairs_for_keys(
        connection, from_type, to_type, method, required_pairs,
    )


def _segment_is_complete(connection, from_type, to_type, method, required_pairs):
    if not required_pairs:
        return True
    required_count = len(required_pairs)
    db_count = get_segment_row_count(connection, from_type, to_type, method)
    if db_count < required_count:
        return False
    return len(_get_missing_pairs(connection, from_type, to_type, method, required_pairs)) == 0


def _build_expected_pair_map(distance_mode, connection, users, clients, visit_csv_path):
    user_ids = set(users.keys())
    client_ids = set(clients.keys())
    if distance_mode == "full":
        if not build_full_matrix_pairs:
            raise RuntimeError("Full distance mode requested but build_full_matrix_pairs is unavailable.")
        return build_full_matrix_pairs(user_ids, client_ids)
    if not build_required_pairs:
        raise RuntimeError("Scoped distance mode requested but build_required_pairs is unavailable.")
    visit_path = resolve_visit_csv_path(visit_csv_path) if resolve_visit_csv_path else None
    return build_required_pairs(connection, visit_csv_path=visit_path)


def _audit_incomplete_segments(connection, scoped_pair_map, segments):
    """Return segments that still have required pairs missing from travel_distances."""
    incomplete = []
    for segment_key, from_type, to_type, _osrm_method, db_method in segments:
        required_pairs = scoped_pair_map.get((from_type, to_type), set()) if scoped_pair_map else set()
        if not required_pairs:
            continue
        required_count = len(required_pairs)
        db_count = get_segment_row_count(connection, from_type, to_type, db_method)
        if db_count < required_count:
            incomplete.append({
                "segment_key": segment_key,
                "from_type": from_type,
                "to_type": to_type,
                "method": db_method,
                "required": required_count,
                "missing": required_count - db_count,
            })
            continue
        missing_count = len(_get_missing_pairs(
            connection, from_type, to_type, db_method, required_pairs,
        ))
        if missing_count:
            incomplete.append({
                "segment_key": segment_key,
                "from_type": from_type,
                "to_type": to_type,
                "method": db_method,
                "required": required_count,
                "missing": missing_count,
            })
    return incomplete


def _log_expected_row_counts(scoped_pair_map, segments):
    if not scoped_pair_map:
        return
    total_rows = 0
    for segment_key, from_type, to_type, _osrm_method, _db_method in segments:
        pair_count = len(scoped_pair_map.get((from_type, to_type), set()))
        total_rows += pair_count
        if pair_count:
            logger.info(
                "  Segment %s: %d row(s) required",
                segment_key,
                pair_count,
            )
    logger.info("Expected travel_distances rows when complete: %d", total_rows)


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
    required_pairs=None,
):
    """Legacy full-matrix segment processor (DISTANCE_MODE=full). Uses pipeline when no cache."""
    if required_pairs is not None:
        return process_scoped_segment(
            connection, cursor, source_entities, target_entities,
            required_pairs, from_type, to_type, osrm_method, db_enum_method,
            state=state, segment_key=segment_key,
        )

    if not missing_source_ids:
        logger.info(f"  {from_type}->{to_type} ({db_enum_method}): No missing pairs.")
        return
    logger.info(f"  {from_type}->{to_type} ({db_enum_method}): Found {len(missing_source_ids)} sources with missing data.")
    matrix = load_cache(osrm_method, from_type, to_type)
    had_partial_errors = False
    if not matrix:
        sources_to_calc = {sid: source_entities[sid] for sid in missing_source_ids if sid in source_entities}
        if not sources_to_calc:
            logger.warning("    No coordinate data found for missing source IDs.")
            return
        logger.info(f"    >>> CALLING OSRM API: {len(sources_to_calc)} x {len(target_entities)}")
        inserter = PipelineInserter(
            connection, cursor, from_type, to_type, db_enum_method,
            state=state, segment_key=segment_key, skip_conflict_check=False,
            async_insert=True,
        )

        def on_block(block_matrix):
            inserter.add_block(block_matrix)

        result = get_distance_matrix(
            entities_info1=sources_to_calc,
            entities_info2=target_entities,
            travel_method=osrm_method,
            step_size=DEFAULT_STEP_SIZE,
            on_block_complete=on_block,
        )
        had_partial_errors = bool(result.get("errors"))
        attempted_rows, batches_committed = inserter.finish()
        if had_partial_errors:
            logger.warning(
                "    OSRM returned partial results: %d failed block(s). "
                "Will insert successful pairs, then stop so retry fills remaining pairs.",
                len(result.get("errors") or []),
            )
        else:
            pass  # pipeline mode: no segment cache (pairs committed per block)
        matrix = None
    else:
        logger.info("    Using cached data (Skipping OSRM API call).")
        attempted_rows, batches_committed = insert_matrix_streaming(
            connection, cursor, matrix, from_type, to_type, db_enum_method,
            state=state, segment_key=segment_key,
        )

    if matrix is None:
        if attempted_rows == 0:
            logger.info("    No missing pairs to insert.")
            if had_partial_errors:
                raise RuntimeError(
                    "OSRM segment had failed block(s). No new successful pairs to insert; retry to calculate remaining missing pairs."
                )
            clear_cache(osrm_method, from_type, to_type)
            return
        logger.info("    Inserted/updated records (batches committed: %d).", batches_committed)
        if had_partial_errors:
            raise RuntimeError(
                "OSRM segment had failed block(s). Inserted successful pairs; retry to calculate remaining missing pairs."
            )
        clear_cache(osrm_method, from_type, to_type)
        return

    if attempted_rows == 0:
        logger.info("    No missing pairs to insert.")
        if had_partial_errors:
            raise RuntimeError(
                "OSRM segment had failed block(s). No new successful pairs to insert; retry to calculate remaining missing pairs."
            )
        clear_cache(osrm_method, from_type, to_type)
        return
    logger.info("    Inserted/updated records (batches committed: %d).", batches_committed)
    if had_partial_errors:
        raise RuntimeError(
            "OSRM segment had failed block(s). Inserted successful pairs; retry to calculate remaining missing pairs."
        )
    clear_cache(osrm_method, from_type, to_type)


def _build_segments_list(scoped_pair_map=None):
    """List of (segment_key, from_type, to_type, osrm_method, db_enum_method) in execution order."""
    out = []
    for osrm_method in OSRM_METHODS:
        db_enum = TRAVEL_METHOD_MAP[osrm_method]
        segment_defs = [
            (ENTITY_TYPE_USER, ENTITY_TYPE_USER),
            (ENTITY_TYPE_CLIENT, ENTITY_TYPE_CLIENT),
            (ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT),
            (ENTITY_TYPE_CLIENT, ENTITY_TYPE_USER),
        ]
        if scoped_pair_map is not None:
            segment_defs = [
                (ft, tt) for ft, tt in segment_defs
                if scoped_pair_map.get((ft, tt))
            ]
        for from_type, to_type in segment_defs:
            out.append((
                _segment_key(osrm_method, from_type, to_type),
                from_type, to_type, osrm_method, db_enum,
            ))
    return out


def process_scoped_segment(
    connection,
    cursor,
    source_entities,
    target_entities,
    required_pairs,
    from_type,
    to_type,
    osrm_method,
    db_enum_method,
    state=None,
    segment_key=None,
):
    """Compute and insert only missing pairs; OSRM and DB insert run in parallel."""
    if not required_pairs:
        logger.info(f"  {from_type}->{to_type} ({db_enum_method}): No required pairs.")
        return True

    missing = _get_missing_pairs(connection, from_type, to_type, db_enum_method, required_pairs)
    if not missing:
        logger.info(
            f"  {from_type}->{to_type} ({db_enum_method}): All {len(required_pairs)} pair(s) already in DB."
        )
        return True

    source_ids = {s for s, _t in missing}
    target_ids = {t for _s, t in missing}
    sources_to_calc = {sid: source_entities[sid] for sid in source_ids if sid in source_entities}
    targets = {tid: target_entities[tid] for tid in target_ids if tid in target_entities}
    if not sources_to_calc or not targets:
        unmapped_sources = sum(1 for s, _t in missing if s not in source_entities)
        unmapped_targets = sum(1 for _s, t in missing if t not in target_entities)
        raise RuntimeError(
            f"Cannot compute {len(missing)} missing pair(s) for "
            f"{from_type}->{to_type} ({db_enum_method}): "
            f"{unmapped_sources} source(s) and {unmapped_targets} target(s) lack coordinates."
        )

    logger.info(
        "  %s->%s (%s): %d missing pair(s), %d sources x %d targets (retry skips %d already in DB)",
        from_type, to_type, db_enum_method, len(missing), len(sources_to_calc), len(targets),
        len(required_pairs) - len(missing),
    )

    inserter = PipelineInserter(
        connection, cursor, from_type, to_type, db_enum_method,
        state=state, segment_key=segment_key, skip_conflict_check=True,
        async_insert=True,
    )

    def on_block(block_matrix):
        inserter.add_block(block_matrix)

    result = get_distance_matrix(
        entities_info1=sources_to_calc,
        entities_info2=targets,
        travel_method=osrm_method,
        step_size=DEFAULT_STEP_SIZE,
        on_block_complete=on_block,
        required_pairs=missing,
    )

    attempted_rows, batches_committed = inserter.finish()
    had_partial_errors = bool(result.get("errors"))
    still_missing = _get_missing_pairs(connection, from_type, to_type, db_enum_method, required_pairs)

    if had_partial_errors:
        logger.warning(
            "    OSRM partial: %d failed block(s). Inserted %d row(s); %d pair(s) still missing (retry continues from here).",
            len(result.get("errors") or []),
            attempted_rows,
            len(still_missing),
        )

    if still_missing:
        if state and segment_key:
            state.update(
                "distance_migration",
                current_segment=segment_key,
                segment_missing_pairs=len(still_missing),
            )
        raise RuntimeError(
            f"Segment incomplete: {len(still_missing)} pair(s) still missing. "
            "Successful pairs are already in DB; retry will skip them."
        )

    logger.info("    Inserted %d row(s) in %d batch(es). Segment complete.", attempted_rows, batches_committed)
    return True


def run(connection_manager=None, state=None, visit_csv_path=None):
    distance_mode = get_distance_mode()
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     Travel Distances Migration (RESUME ENABLED)          ║
    ║     Full matrix by default -> OSRM pipeline -> COPY      ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    logger.info("Cache directory: %s", CACHE_DIR)
    logger.info("Distance mode: %s", distance_mode)
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
        try:
            cursor.execute("SET synchronous_commit = off")
        except Exception:
            pass

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
        logger.info(
            "Entities with coordinates: %d users, %d clients",
            len(user_ids),
            len(client_ids),
        )

        scoped_pair_map = None
        if distance_mode in ("scoped", "full"):
            scoped_pair_map = _build_expected_pair_map(
                distance_mode, connection, users, clients, visit_csv_path,
            )
            if distance_mode == "scoped":
                visit_path = resolve_visit_csv_path(visit_csv_path) if resolve_visit_csv_path else None
                if visit_path:
                    logger.info("VisitExport for route pairs: %s", visit_path)
                else:
                    logger.warning("VisitExport not found; client↔client route pairs will be omitted.")
            else:
                logger.info(
                    "Full matrix mode: %d users x %d clients (+ user↔user, client↔client)",
                    len(user_ids),
                    len(client_ids),
                )
        else:
            raise ValueError(
                f"Unknown DISTANCE_MODE={distance_mode!r}. Use 'full' (default) or 'scoped'."
            )

        segments = _build_segments_list(scoped_pair_map)
        _log_expected_row_counts(scoped_pair_map, segments)
        for segment_key, from_type, to_type, osrm_method, db_enum_method in segments:
            required_pairs = None
            if scoped_pair_map is not None:
                required_pairs = scoped_pair_map.get((from_type, to_type), set())

            if segment_key in completed_segments:
                if required_pairs and not _segment_is_complete(
                    connection, from_type, to_type, db_enum_method, required_pairs,
                ):
                    remaining = len(_get_missing_pairs(
                        connection, from_type, to_type, db_enum_method, required_pairs,
                    ))
                    logger.info(
                        "Segment %s marked complete but %d pair(s) missing; resuming.",
                        segment_key, remaining,
                    )
                else:
                    logger.info("Skipping completed segment: %s", segment_key)
                    continue

            source_entities = users if from_type == ENTITY_TYPE_USER else clients
            target_entities = users if to_type == ENTITY_TYPE_USER else clients
            if not source_entities or not target_entities:
                continue

            if required_pairs is not None:
                logger.info(
                    "Checking segment %s (%d required pair(s))...",
                    segment_key,
                    len(required_pairs),
                )

            missing_src = set()
            if scoped_pair_map is None:
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
                required_pairs=required_pairs,
            )

            if state:
                if segment_key not in completed_segments:
                    completed_segments = list(completed_segments) + [segment_key]
                state.update("distance_migration", completed_segments=completed_segments, current_segment=segment_key)

        incomplete = _audit_incomplete_segments(connection, scoped_pair_map, segments)
        if incomplete:
            for seg in incomplete:
                logger.error(
                    "INCOMPLETE: %s (%s->%s %s): %d/%d pair(s) missing",
                    seg["segment_key"],
                    seg["from_type"],
                    seg["to_type"],
                    seg["method"],
                    seg["missing"],
                    seg["required"],
                )
            total_missing = sum(seg["missing"] for seg in incomplete)
            raise RuntimeError(
                f"Distance migration incomplete: {total_missing} required pair(s) still missing "
                f"across {len(incomplete)} segment(s). Re-run to resume; pairs already inserted are skipped."
            )

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
        if state:
            state.update("distance_migration", completed_segments=completed_segments)
        logger.exception("Migration failed: %s", e)
        logger.error(
            "Retry will resume from the last incomplete segment; pairs already inserted are skipped."
        )
        return False
    finally:
        if connection and not connection_manager and connection.closed == 0:
            connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)