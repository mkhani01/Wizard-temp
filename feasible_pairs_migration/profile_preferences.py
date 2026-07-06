"""
Profile-level Must / Preferred / Only preferences from feasible pair weights and visit status.

Mirrors server PreferenceSyncService profile rules: mutually exclusive categories,
two-way sync between user_* and client_* join tables.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

try:
    from psycopg2.extras import execute_values
    from psycopg2 import OperationalError, InterfaceError
except ImportError:
    execute_values = None
    OperationalError = Exception
    InterfaceError = Exception

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

logger = logging.getLogger(__name__)

WEIGHT_THRESHOLD = 0.9
LONG_DURATION_MINUTES = 300
PREFERRED_STATUS = "Current Primary"

PROFILE_TABLES = {
    "preferred": ("user_preferred_clients", "client_preferred_users"),
    "must": ("user_must_clients", "client_must_users"),
    "only": ("user_only_clients", "client_only_users"),
}

ALL_PROFILE_TABLES = [
    "user_preferred_clients",
    "user_must_clients",
    "user_only_clients",
    "client_preferred_users",
    "client_must_users",
    "client_only_users",
]


def load_client_durations(connection) -> Dict[int, int]:
    """Max requested_duration (minutes) per client from client_schedules."""
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT client_id, MAX(COALESCE(requested_duration, 0)) AS duration_minutes
            FROM client_schedules
            WHERE deleted_at IS NULL
            GROUP BY client_id
        """)
        return {int(row["client_id"]): int(row["duration_minutes"] or 0) for row in cursor.fetchall()}
    except Exception as e:
        logger.warning("Could not load client durations (client_schedules may be empty): %s", e)
        return {}
    finally:
        cursor.close()


def classify_profile_category(
    weight: float,
    client_duration_minutes: int,
    carer_status: Optional[str],
) -> Optional[str]:
    """
    Return 'only', 'must', 'preferred', or None.
    ONLY/MUST take precedence over PREFERRED (only_patient.py logic).
    """
    if weight >= WEIGHT_THRESHOLD:
        if client_duration_minutes >= LONG_DURATION_MINUTES:
            return "only"
        return "must"
    if carer_status == PREFERRED_STATUS:
        return "preferred"
    return None


def classify_pairs(
    weights: Dict[Tuple[int, int], float],
    statuses: Dict[Tuple[int, int], str],
    client_durations: Dict[int, int],
) -> Dict[str, List[Tuple[int, int, float]]]:
    """
    Classify (caregiver_id, client_id) pairs into profile categories.
    Returns dict category -> list of (user_id, client_id, weight).
    """
    categorized: Dict[str, List[Tuple[int, int, float]]] = {
        "preferred": [],
        "must": [],
        "only": [],
    }
    for (user_id, client_id), weight in weights.items():
        status = statuses.get((user_id, client_id))
        duration = client_durations.get(client_id, 0)
        category = classify_profile_category(float(weight), duration, status)
        if category:
            categorized[category].append((user_id, client_id, float(weight)))
    return categorized


def _assign_sort_orders(rows: List[Tuple[int, int, float]], group_by_index: int) -> List[Tuple[int, int, int]]:
    """Assign sort_order per entity within category, descending by weight."""
    by_entity: Dict[int, List[Tuple[int, int, float]]] = defaultdict(list)
    for row in rows:
        by_entity[row[group_by_index]].append(row)

    result: List[Tuple[int, int, int]] = []
    for _entity_id, entity_rows in by_entity.items():
        sorted_rows = sorted(entity_rows, key=lambda r: r[2], reverse=True)
        for sort_order, (user_id, client_id, _weight) in enumerate(sorted_rows, start=1):
            result.append((user_id, client_id, sort_order))
    return result


def build_profile_rows(
    categorized: Dict[str, List[Tuple[int, int, float]]],
) -> Dict[str, List[Tuple]]:
    """
    Build insert rows for all six tables with sort_order.
    User tables: (user_id, client_id, sort_order)
    Client tables: (client_id, user_id, sort_order)
    """
    out: Dict[str, List[Tuple]] = {t: [] for t in ALL_PROFILE_TABLES}

    for category, (user_table, client_table) in PROFILE_TABLES.items():
        rows = categorized.get(category, [])
        if not rows:
            continue
        user_rows = _assign_sort_orders(rows, group_by_index=0)
        client_rows = _assign_sort_orders(rows, group_by_index=1)
        out[user_table] = user_rows
        out[client_table] = [(cid, uid, so) for uid, cid, so in client_rows]

    return out


def refresh_all_profile_preferences(
    connection,
    weights: Dict[Tuple[int, int], float],
    statuses: Dict[Tuple[int, int], str],
    client_durations: Optional[Dict[int, int]] = None,
) -> Dict[str, int]:
    """
    Full refresh of all six profile preference join tables (DELETE + INSERT).
    Returns counts per table.
    """
    if client_durations is None:
        client_durations = load_client_durations(connection)

    categorized = classify_pairs(weights, statuses, client_durations)
    table_rows = build_profile_rows(categorized)

    counts = {cat: len(categorized[cat]) for cat in ("preferred", "must", "only")}
    logger.info(
        "Profile classification: preferred=%d, must=%d, only=%d",
        counts["preferred"],
        counts["must"],
        counts["only"],
    )

    cursor = connection.cursor()
    try:
        logger.info("Refreshing profile preference tables (DELETE + INSERT, two-way sync)...")
        for table in ALL_PROFILE_TABLES:
            cursor.execute(f"DELETE FROM {table}")

        for table in ALL_PROFILE_TABLES:
            rows = table_rows.get(table, [])
            if not rows:
                continue
            if table.startswith("user_"):
                insert_sql = f"INSERT INTO {table} (user_id, client_id, sort_order) VALUES %s"
                template = "(%s, %s, %s)"
            else:
                insert_sql = f"INSERT INTO {table} (client_id, user_id, sort_order) VALUES %s"
                template = "(%s, %s, %s)"
            execute_values(cursor, insert_sql, rows, template=template)

        connection.commit()
        result = {table: len(table_rows.get(table, [])) for table in ALL_PROFILE_TABLES}
        for table, cnt in result.items():
            if cnt:
                logger.info("  %s: %d row(s)", table, cnt)
        return result
    except (OperationalError, InterfaceError) as e:
        connection.rollback()
        if ConnectionLostError:
            raise ConnectionLostError("profile_preferences", {}) from e
        raise
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def load_profile_user_client_pairs(connection) -> Set[Tuple[int, int]]:
    """All (user_id, client_id) pairs from profile Must/Preferred/Only tables."""
    pairs: Set[Tuple[int, int]] = set()
    cursor = connection.cursor()
    try:
        for user_table, _client_table in PROFILE_TABLES.values():
            cursor.execute(f"SELECT user_id, client_id FROM {user_table}")
            for row in cursor.fetchall():
                pairs.add((int(row["user_id"]), int(row["client_id"])))
    finally:
        cursor.close()
    return pairs
