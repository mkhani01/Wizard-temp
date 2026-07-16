"""
Update Today Visits Migration
=============================
Cancel roster visits for a selected calendar date using Client Hours with Service Type:

1. Rows with non-empty Cancellation Description → cancel matching ALLOCATED/UNALLOCATED
   visits (client + start/end minutes) with that cancellation type (insert type if missing).
2. All ALLOCATED/UNALLOCATED visits for terminated clients on that date → cancel with
   type "Terminated" (insert if missing).

Missing visits / missing roster → skip and log.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime, timedelta, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    import openpyxl
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Please install: pip install psycopg2-binary openpyxl")
    sys.exit(1)

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

from encoding_utils import fix_utf8_mojibake, normalize_name_for_match

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler("migration_update_today_visits.log", mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

TERMINATED_CANCELLATION_TYPE = "Terminated"
ACTIVE_VISIT_STATUSES = ("UNALLOCATED", "ALLOCATED")


class MigrationError(Exception):
    pass


def get_db_config() -> Dict[str, Any]:
    config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise MigrationError(f"Missing database configuration: {missing}")
    return config


def connect_to_database(config: Dict[str, Any]):
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
        keepalives_count=5,
    )
    connection.autocommit = False
    return connection


def parse_datetime_value(datetime_val) -> Optional[datetime]:
    if datetime_val is None:
        return None
    if isinstance(datetime_val, datetime):
        return datetime_val
    if isinstance(datetime_val, date) and not isinstance(datetime_val, datetime):
        return datetime.combine(datetime_val, time.min)
    if isinstance(datetime_val, str):
        datetime_str = datetime_val.strip()
        if not datetime_str:
            return None
        for fmt in (
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M",
            "%Y-%m-%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
        ):
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue
    if isinstance(datetime_val, (int, float)):
        try:
            base = datetime(1899, 12, 30)
            return base + timedelta(days=float(datetime_val))
        except (OverflowError, ValueError):
            pass
    return None


def datetime_to_minutes(dt: datetime) -> int:
    return dt.hour * 60 + dt.minute


def parse_target_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
    raise MigrationError(f"Invalid target date: {value!r} (expected YYYY-MM-DD)")


def resolve_start_end(
    req_start_val,
    req_end_val,
    act_start_val,
    act_end_val,
) -> Tuple[Optional[datetime], Optional[datetime], str]:
    """
    Prefer Requirement start/end; fall back to Actual per side.
    Returns (start, end, source_label).
    """
    req_start = parse_datetime_value(req_start_val)
    req_end = parse_datetime_value(req_end_val)
    act_start = parse_datetime_value(act_start_val)
    act_end = parse_datetime_value(act_end_val)

    start = req_start or act_start
    end = req_end or act_end
    if req_start and req_end:
        source = "requirement"
    elif act_start and act_end and not (req_start or req_end):
        source = "actual"
    else:
        source = "mixed"
    return start, end, source


def get_all_clients(connection) -> Dict[str, int]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id, name, lastname FROM client WHERE deleted_at IS NULL")
        clients: Dict[str, int] = {}
        for row in cursor.fetchall():
            name = (row["name"] or "").strip()
            lastname = (row["lastname"] or "").strip()
            client_id = row["id"]
            key_comma = normalize_name_for_match(f"{lastname}, {name}")
            key_space = normalize_name_for_match(f"{name} {lastname}")
            if key_comma:
                if key_comma not in clients or client_id > clients[key_comma]:
                    clients[key_comma] = client_id
            if key_space:
                if key_space not in clients or client_id > clients[key_space]:
                    clients[key_space] = client_id
        return clients
    finally:
        cursor.close()


def ensure_cancellation_types(connection, names: Sequence[str]) -> Dict[str, int]:
    """
    Ensure each name exists in cancellation_types (is_paid=false for inserts).
    Returns name -> id map for all requested names (plus any already present).
    """
    unique_names = sorted({(n or "").strip() for n in names if (n or "").strip()})
    if not unique_names:
        return {}

    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id, name FROM cancellation_types WHERE name = ANY(%s)", (unique_names,))
        existing = {row["name"]: row["id"] for row in cursor.fetchall()}
        missing = [n for n in unique_names if n not in existing]
        for name in missing:
            cursor.execute(
                """
                INSERT INTO cancellation_types (name, is_paid, created_date, last_modified_date)
                VALUES (%s, false, NOW(), NOW())
                ON CONFLICT (name) DO NOTHING
                RETURNING id, name
                """,
                (name,),
            )
            row = cursor.fetchone()
            if row:
                existing[row["name"]] = row["id"]
                logger.info("Inserted cancellation type %r (id=%s)", name, row["id"])
            else:
                cursor.execute("SELECT id, name FROM cancellation_types WHERE name = %s", (name,))
                found = cursor.fetchone()
                if found:
                    existing[found["name"]] = found["id"]
        connection.commit()
        return existing
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def load_roster_visits_for_date(connection, target_date: date) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id FROM roster WHERE date = %s", (target_date,))
        roster = cursor.fetchone()
        if not roster:
            return None, []
        roster_id = roster["id"]
        cursor.execute(
            """
            SELECT id, receiver_client_id, start_minute, end_minute, status, cancellation_type_id
            FROM roster_visit
            WHERE roster_id = %s
              AND receiver_type = 'CLIENT'
              AND receiver_client_id IS NOT NULL
            """,
            (roster_id,),
        )
        return roster_id, list(cursor.fetchall())
    finally:
        cursor.close()


def get_terminated_client_ids(connection, target_date: date) -> Set[int]:
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT id
            FROM client
            WHERE deleted_at IS NULL
              AND (
                status = 'Deactive'
                OR (termination_date IS NOT NULL AND termination_date < %s)
              )
            """,
            (target_date,),
        )
        return {int(row["id"]) for row in cursor.fetchall()}
    finally:
        cursor.close()


def _col_idx(headers: Sequence[str], names: Sequence[str]) -> int:
    lower_headers = [str(h or "").strip().lower() for h in headers]
    for name in names:
        nl = name.lower()
        for i, h in enumerate(lower_headers):
            if h == nl:
                return i
    return -1


def extract_cancellation_rows(
    filepath: Path,
    target_date: date,
    clients_map: Dict[str, int],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Parse Client Hours XLSX Data sheet; return cancellation rows for target_date
    and stats counters.
    """
    stats = {
        "total_rows": 0,
        "with_cancellation": 0,
        "on_target_date": 0,
        "skipped_missing_datetime": 0,
        "skipped_unknown_client": 0,
        "skipped_wrong_date": 0,
    }
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    try:
        if "Data" not in wb.sheetnames:
            raise MigrationError("Sheet 'Data' not found in workbook")
        ws = wb["Data"]
        rows_iter = ws.iter_rows(values_only=True)
        header = next(rows_iter, None)
        if not header:
            raise MigrationError("Workbook has no header row")

        col_loc = _col_idx(header, ["Service Location Name"])
        col_req_start = _col_idx(header, ["Service Requirement Start Date And Time"])
        col_req_end = _col_idx(header, ["Service Requirement End Date And Time"])
        col_act_start = _col_idx(header, ["Actual Start Date And Time"])
        col_act_end = _col_idx(header, ["Actual End Date And Time"])
        col_cancel = _col_idx(header, ["Cancellation Description"])

        if col_loc == -1:
            raise MigrationError("Missing required column: Service Location Name")
        if col_cancel == -1:
            raise MigrationError("Missing required column: Cancellation Description")
        if col_req_start == -1 and col_act_start == -1:
            raise MigrationError(
                "Missing start datetime columns (need Requirement and/or Actual Start)"
            )
        if col_req_end == -1 and col_act_end == -1:
            raise MigrationError(
                "Missing end datetime columns (need Requirement and/or Actual End)"
            )

        results: List[Dict[str, Any]] = []
        for row_num, row in enumerate(rows_iter, start=2):
            stats["total_rows"] += 1
            if not row:
                continue
            cancel_val = row[col_cancel] if col_cancel < len(row) else None
            cancel_name = str(cancel_val).strip() if cancel_val is not None else ""
            if not cancel_name or cancel_name.lower() == "none":
                continue
            stats["with_cancellation"] += 1

            raw_loc = row[col_loc] if col_loc < len(row) else None
            loc = fix_utf8_mojibake(raw_loc) if raw_loc is not None else None
            loc_str = str(loc).strip() if loc is not None else ""
            if not loc_str:
                logger.warning("Row %d: SKIPPED - empty Service Location Name", row_num)
                continue

            client_key = normalize_name_for_match(loc_str)
            client_id = clients_map.get(client_key)
            if not client_id:
                stats["skipped_unknown_client"] += 1
                logger.warning(
                    "Row %d: SKIPPED - client not found | Location=%r Cancellation=%r",
                    row_num,
                    loc_str,
                    cancel_name,
                )
                continue

            req_start = row[col_req_start] if col_req_start != -1 and col_req_start < len(row) else None
            req_end = row[col_req_end] if col_req_end != -1 and col_req_end < len(row) else None
            act_start = row[col_act_start] if col_act_start != -1 and col_act_start < len(row) else None
            act_end = row[col_act_end] if col_act_end != -1 and col_act_end < len(row) else None

            start_dt, end_dt, source = resolve_start_end(req_start, req_end, act_start, act_end)
            if not start_dt or not end_dt:
                stats["skipped_missing_datetime"] += 1
                logger.warning(
                    "Row %d: SKIPPED - missing datetime (Requirement and Actual empty) | Location=%r",
                    row_num,
                    loc_str,
                )
                continue

            visit_date = start_dt.date()
            if visit_date != target_date:
                stats["skipped_wrong_date"] += 1
                continue

            stats["on_target_date"] += 1
            results.append(
                {
                    "row_num": row_num,
                    "client_id": client_id,
                    "client_name": loc_str,
                    "cancellation_name": cancel_name,
                    "start_minute": datetime_to_minutes(start_dt),
                    "end_minute": datetime_to_minutes(end_dt),
                    "source": source,
                }
            )
        return results, stats
    finally:
        wb.close()


def match_and_cancel_from_file(
    connection,
    visits: List[Dict[str, Any]],
    cancel_rows: List[Dict[str, Any]],
    type_ids: Dict[str, int],
) -> Tuple[int, int]:
    """
    Cancel visits matching file rows. Returns (cancelled_count, skipped_unmatched).
    """
    # Index active visits by (client_id, start_minute, end_minute)
    index: Dict[Tuple[int, int, int], List[Dict[str, Any]]] = {}
    for v in visits:
        if v["status"] not in ACTIVE_VISIT_STATUSES:
            continue
        key = (int(v["receiver_client_id"]), int(v["start_minute"]), int(v["end_minute"]))
        index.setdefault(key, []).append(v)

    cancelled = 0
    skipped = 0
    cursor = connection.cursor()
    try:
        for row in cancel_rows:
            type_id = type_ids.get(row["cancellation_name"])
            if type_id is None:
                logger.warning(
                    "Row %d: SKIPPED - cancellation type id missing for %r",
                    row["row_num"],
                    row["cancellation_name"],
                )
                skipped += 1
                continue
            key = (row["client_id"], row["start_minute"], row["end_minute"])
            matches = index.get(key) or []
            if not matches:
                skipped += 1
                logger.warning(
                    "Row %d: SKIPPED - no matching roster visit | client_id=%s start=%s end=%s cancel=%r",
                    row["row_num"],
                    row["client_id"],
                    row["start_minute"],
                    row["end_minute"],
                    row["cancellation_name"],
                )
                continue
            # Cancel all matching slots (e.g. multi-caregiver)
            still_active = [m for m in matches if m["status"] in ACTIVE_VISIT_STATUSES]
            if not still_active:
                logger.info(
                    "Row %d: visit(s) already cancelled | client_id=%s start=%s end=%s",
                    row["row_num"],
                    row["client_id"],
                    row["start_minute"],
                    row["end_minute"],
                )
                continue
            for visit in still_active:
                cursor.execute(
                    """
                    UPDATE roster_visit
                    SET status = 'CANCELLED',
                        cancellation_type_id = %s,
                        updated_at = NOW()
                    WHERE id = %s
                      AND status = ANY(%s)
                    """,
                    (type_id, visit["id"], list(ACTIVE_VISIT_STATUSES)),
                )
                if cursor.rowcount:
                    visit["status"] = "CANCELLED"
                    visit["cancellation_type_id"] = type_id
                    cancelled += 1
                    logger.info(
                        "Cancelled visit %s from file | client_id=%s type=%r",
                        visit["id"],
                        row["client_id"],
                        row["cancellation_name"],
                    )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
    return cancelled, skipped


def cancel_terminated_client_visits(
    connection,
    visits: List[Dict[str, Any]],
    terminated_ids: Set[int],
    terminated_type_id: int,
) -> int:
    cancelled = 0
    cursor = connection.cursor()
    try:
        for visit in visits:
            if visit["status"] not in ACTIVE_VISIT_STATUSES:
                continue
            client_id = int(visit["receiver_client_id"])
            if client_id not in terminated_ids:
                continue
            cursor.execute(
                """
                UPDATE roster_visit
                SET status = 'CANCELLED',
                    cancellation_type_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND status = ANY(%s)
                """,
                (terminated_type_id, visit["id"], list(ACTIVE_VISIT_STATUSES)),
            )
            if cursor.rowcount:
                visit["status"] = "CANCELLED"
                visit["cancellation_type_id"] = terminated_type_id
                cancelled += 1
                logger.info(
                    "Cancelled visit %s for terminated client_id=%s with type %r",
                    visit["id"],
                    client_id,
                    TERMINATED_CANCELLATION_TYPE,
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
    return cancelled


def run(
    excel_path: Optional[str] = None,
    target_date: Optional[Any] = None,
    connection_manager=None,
    state=None,
) -> bool:
    print(
        """
    ╔════════════════════════════════════════════════════════════════╗
    ║   UPDATE TODAY VISITS                                          ║
    ║   Cancel visits from Cancellation Description + Terminated     ║
    ╚════════════════════════════════════════════════════════════════╝
    """
    )
    if state and state.is_completed("update_today_visits"):
        logger.info("Update today visits already completed (resume).")
        return True

    from migration_support import get_assets_dir

    filepath = (
        Path(excel_path)
        if excel_path
        else get_assets_dir() / "updateTodayVisits" / "ClientHoursWithServiceType.xlsx"
    )
    if not filepath.exists():
        logger.error("Excel file not found: %s", filepath)
        return False

    if target_date is None:
        target_date = date.today()
    try:
        parsed_date = parse_target_date(target_date)
    except MigrationError as e:
        logger.error("%s", e)
        return False

    logger.info("Excel: %s | target date: %s", filepath, parsed_date)
    connection = None
    try:
        config = get_db_config()
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
            connection = connect_to_database(config)

        clients_map = get_all_clients(connection)
        cancel_rows, extract_stats = extract_cancellation_rows(filepath, parsed_date, clients_map)
        logger.info(
            "Extract stats: total=%s with_cancel=%s on_date=%s missing_dt=%s unknown_client=%s wrong_date=%s",
            extract_stats["total_rows"],
            extract_stats["with_cancellation"],
            extract_stats["on_target_date"],
            extract_stats["skipped_missing_datetime"],
            extract_stats["skipped_unknown_client"],
            extract_stats["skipped_wrong_date"],
        )

        type_names = [r["cancellation_name"] for r in cancel_rows]
        type_names.append(TERMINATED_CANCELLATION_TYPE)
        type_ids = ensure_cancellation_types(connection, type_names)
        terminated_type_id = type_ids.get(TERMINATED_CANCELLATION_TYPE)
        if terminated_type_id is None:
            raise MigrationError("Failed to ensure Terminated cancellation type")

        roster_id, visits = load_roster_visits_for_date(connection, parsed_date)
        if not roster_id:
            logger.warning(
                "No roster found for %s — nothing to cancel. Exiting successfully.",
                parsed_date,
            )
            if state:
                state.clear_step("update_today_visits")
            return True

        logger.info("Roster %s has %d client visits", roster_id, len(visits))
        file_cancelled, file_skipped = match_and_cancel_from_file(
            connection, visits, cancel_rows, type_ids
        )

        terminated_ids = get_terminated_client_ids(connection, parsed_date)
        term_cancelled = cancel_terminated_client_visits(
            connection, visits, terminated_ids, terminated_type_id
        )

        print("\n" + "=" * 60)
        print("✓ UPDATE TODAY VISITS COMPLETED")
        print("=" * 60)
        print(f"  Target date: {parsed_date}")
        print(f"  Cancellation rows on date: {len(cancel_rows)}")
        print(f"  Cancelled from file: {file_cancelled}")
        print(f"  Skipped unmatched file rows: {file_skipped}")
        print(f"  Terminated clients: {len(terminated_ids)}")
        print(f"  Cancelled for terminated: {term_cancelled}")

        if state:
            state.clear_step("update_today_visits")
        return True
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        if ConnectionLostError:
            raise ConnectionLostError("update_today_visits", {}) from e
        raise
    except MigrationError as e:
        logger.error("Migration error: %s", e)
        return False
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        return False
    finally:
        if connection and not connection_manager:
            connection.close()


if __name__ == "__main__":
    excel_arg = sys.argv[1] if len(sys.argv) > 1 else None
    date_arg = sys.argv[2] if len(sys.argv) > 2 else None
    success = run(excel_path=excel_arg, target_date=date_arg)
    sys.exit(0 if success else 1)
