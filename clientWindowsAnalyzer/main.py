"""
Client Windows Analyzer
Updates existing client_availabilities records with optimized start_time, end_time, and minDuration
computed from historical visit data (VisitExport-style CSV).

Matching Logic:
For each client_availability record, finds all CSV visits that are "covered" by it using:
1. Client ID match
2. Day of week match (CSV visit day must be in availability.days array)
3. Time slot match (with 10-minute tolerance)
4. Date falls within recurrence pattern (using start_date and occurs_every)

This ensures CSV visits are properly matched even if they occur on different dates within
the same recurrence pattern (e.g., weekly or bi-weekly schedules).

Important Rules - Records are SKIPPED and left with NULL start_time, end_time, and minDuration if:
1. number_of_care_givers >= 2 (multiple caregivers needed simultaneously)
2. Time slots overlap on the same day for the same client (detected by checking if two time ranges
   on shared days overlap)
3. No CSV visits match the availability pattern (day/time/recurrence mismatch)

Only non-overlapping records with number_of_care_givers = 1 are analyzed and updated.
minDuration is always clamped to not exceed the Service Requirement Duration.
"""

import os
import re
import sys
import logging
from pathlib import Path
from datetime import datetime, time
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any

try:
    import pandas as pd
    import numpy as np
    import psycopg2
    from psycopg2.extras import RealDictCursor, execute_batch
except ImportError as e:
    print("Missing required package: %s" % e)
    print("Please install: pip install psycopg2-binary pandas numpy")
    sys.exit(1)

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

from encoding_utils import fix_utf8_mojibake, normalize_name_for_match

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIMUM_DURATION_MINUTES = 20
LOWER_PERCENTILE = 0.10
UPPER_PERCENTILE = 0.90
MIN_WINDOW_WIDTH_MINS = 60
TOLERANCE_MINS = 15
FLEXIBILITY_THRESHOLD = 10
DURATION_SIGNIFICANCE_THRESHOLD = 0.10  # Stage 3.7: duration must appear in >= 10% of records

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.FileHandler("client_windows_analyzer.log", mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class MigrationError(Exception):
    pass


# ============================================================================
# DB HELPERS (same pattern as clientAvailabilityMigration)
# ============================================================================

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
    try:
        logger.info(f"Connecting to PostgreSQL at {config['host']}:{config['port']}/{config['database']}...")
        conn = psycopg2.connect(
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
        conn.autocommit = False
        logger.info("✓ Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"✗ Failed to connect to database: {e}")
        raise MigrationError(f"Database connection failed: {e}")


def get_all_clients(connection) -> Dict[str, int]:
    """
    Build lookup: key -> client id.
    Key format: "lastname, name" (lowercase, single space after comma).
    So DB row (name='Harry', lastname='Hawkshaw (DS)') -> key 'hawkshaw (ds), harry'.
    CSV "Service Location Name" must match this exactly (e.g. "Hawkshaw (DS), Harry").
    """
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id, name, lastname FROM client WHERE deleted_at IS NULL")
        clients = {}
        for row in cursor.fetchall():
            name = (row["name"] or "").strip()
            lastname = (row["lastname"] or "").strip()
            key = normalize_name_for_match(f"{lastname}, {name}")
            if key:
                clients[key] = row["id"]
        logger.info(f"✓ Loaded {len(clients)} clients from database")
        return clients
    finally:
        cursor.close()


def _normalize_time_to_hhmmss(t) -> str:
    """Normalize DB time to HH:MM:SS for matching (Patient_Analyzer uses HH:MM in JSON; DB may return HH:MM or HH:MM:SS)."""
    if t is None:
        return ""
    if hasattr(t, "strftime"):
        return t.strftime("%H:%M:%S") if t else ""
    s = str(t).strip()
    if not s:
        return ""
    # Already HH:MM:SS or HH:MM:SS.xxx
    if len(s) >= 8 and s[2] == ":" and s[5] == ":":
        return s[:8]  # HH:MM:SS
    # HH:MM -> HH:MM:00
    if len(s) == 5 and s[2] == ":":
        return s + ":00"
    return s


def _times_overlap(start1_str: str, end1_str: str, start2_str: str, end2_str: str) -> bool:
    """Check if two time ranges overlap. Times are HH:MM:SS strings."""
    def time_to_minutes(t_str: str) -> int:
        """Convert HH:MM:SS to minutes from midnight."""
        if not t_str:
            return 0
        parts = t_str.split(':')
        h = int(parts[0]) if len(parts) > 0 else 0
        m = int(parts[1]) if len(parts) > 1 else 0
        return h * 60 + m

    start1 = time_to_minutes(start1_str)
    end1 = time_to_minutes(end1_str)
    start2 = time_to_minutes(start2_str)
    end2 = time_to_minutes(end2_str)

    # Two ranges overlap if one starts before the other ends
    return start1 < end2 and start2 < end1


def load_client_availabilities(connection) -> Tuple[List[Dict], Dict[int, str]]:
    """Load existing client_availabilities: id, client_id, requested_start_time, requested_end_time, duration, number_of_care_givers.
    Skip records that have:
    1. number_of_care_givers >= 2 (multiple caregivers needed)
    2. Overlapping time slots on the same day for the same client

    Returns:
        Tuple of (records_to_analyze, skip_reasons_dict)
        skip_reasons_dict: {availability_id: reason_string}
    """
    cursor = connection.cursor()
    try:
        # Count total records
        cursor.execute("""
            SELECT COUNT(*) as total FROM client_availabilities WHERE deleted_at IS NULL
        """)
        total_count = cursor.fetchone()["total"]

        # Load ALL records first to detect overlaps (now including start_date and occurs_every for matching)
        cursor.execute("""
            SELECT id, client_id, requested_start_time, requested_end_time,
                   duration, number_of_care_givers, days, start_date, occurs_every
            FROM client_availabilities
            WHERE deleted_at IS NULL
        """)
        all_rows = cursor.fetchall()

        # Convert to list with normalized times
        all_records = []
        for r in all_rows:
            rs = _normalize_time_to_hhmmss(r["requested_start_time"])
            re_ = _normalize_time_to_hhmmss(r["requested_end_time"])

            # Parse days field - PostgreSQL array type may come as list or need parsing
            days_raw = r.get("days", [])
            if isinstance(days_raw, str):
                # PostgreSQL array came as string like "{Tuesday,Friday}"
                days_list = days_raw.strip('{}').split(',') if days_raw else []
                days_list = [d.strip() for d in days_list if d.strip()]
            elif isinstance(days_raw, list):
                days_list = days_raw
            else:
                days_list = []

            all_records.append({
                "id": r["id"],
                "client_id": r["client_id"],
                "requested_start_time": rs,
                "requested_end_time": re_,
                "duration": r.get("duration"),
                "number_of_care_givers": r.get("number_of_care_givers", 1),
                "days": days_list,
                "start_date": r.get("start_date"),
                "occurs_every": r.get("occurs_every", 1),
            })

        # Detect overlaps: for each client, check if any two time slots overlap on the same day
        overlapping_ids = set()
        multiple_caregiver_ids = set()
        skip_reasons = {}  # Track reason for each skipped record

        # Group by client_id
        client_records = defaultdict(list)
        for rec in all_records:
            client_records[rec["client_id"]].append(rec)

        # For each client, check for overlapping time slots on the same days
        for client_id, records in client_records.items():
            # Check each pair of records
            for i in range(len(records)):
                rec1 = records[i]

                # Skip if already marked or has multiple caregivers
                if rec1["number_of_care_givers"] >= 2:
                    multiple_caregiver_ids.add(rec1["id"])
                    skip_reasons[rec1["id"]] = f"Multiple caregivers required (number_of_care_givers={rec1['number_of_care_givers']})"
                    continue

                # Check if this record overlaps with any other record on the same day
                for j in range(i + 1, len(records)):
                    rec2 = records[j]

                    # Check if they share any day of week
                    days1 = set(rec1["days"]) if rec1["days"] else set()
                    days2 = set(rec2["days"]) if rec2["days"] else set()
                    shared_days = days1 & days2

                    if shared_days:
                        # They share a day - check if times overlap
                        if _times_overlap(
                            rec1["requested_start_time"], rec1["requested_end_time"],
                            rec2["requested_start_time"], rec2["requested_end_time"]
                        ):
                            # Mark both as overlapping
                            overlapping_ids.add(rec1["id"])
                            overlapping_ids.add(rec2["id"])
                            skip_reasons[rec1["id"]] = f"Overlapping time slots on same day (shared days: {sorted(shared_days)})"
                            skip_reasons[rec2["id"]] = f"Overlapping time slots on same day (shared days: {sorted(shared_days)})"
                            logger.debug(
                                f"  Overlap detected: client_id={client_id}, "
                                f"slot1={rec1['requested_start_time']}-{rec1['requested_end_time']}, "
                                f"slot2={rec2['requested_start_time']}-{rec2['requested_end_time']}, "
                                f"shared_days={shared_days}"
                            )

        # Filter out records with multiple caregivers or overlaps
        out = []
        skipped_multiple = 0
        skipped_overlap = 0

        for rec in all_records:
            if rec["id"] in multiple_caregiver_ids:
                skipped_multiple += 1
                continue
            if rec["id"] in overlapping_ids:
                skipped_overlap += 1
                continue
            # Only include if number_of_care_givers = 1 and no overlaps
            if rec["number_of_care_givers"] <= 1:
                out.append(rec)

        logger.info(f"✓ Loaded {len(out)} client_availabilities records for analysis")
        if skipped_multiple > 0:
            logger.info(f"  Skipped {skipped_multiple} records with multiple caregivers (number_of_care_givers >= 2)")
        if skipped_overlap > 0:
            logger.info(f"  Skipped {skipped_overlap} records with overlapping time slots on the same day")
        logger.info(f"  Total client_availabilities: {total_count}")

        return out, skip_reasons
    finally:
        cursor.close()


# ============================================================================
# PARSING HELPERS
# ============================================================================

def parse_flexible_datetime(val) -> Optional[datetime]:
    """Parse datetime; primary format %d/%m/%Y %H:%M:%S."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()
    if not s:
        return None
    for fmt in ["%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M"]:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def min_to_time_str(minutes_from_midnight: int) -> str:
    """Convert minutes from midnight to HH:MM:SS (PostgreSQL TIME format)."""
    minutes_from_midnight = max(0, min(24 * 60 - 1, int(minutes_from_midnight)))
    h = minutes_from_midnight // 60
    m = minutes_from_midnight % 60
    return f"{h:02d}:{m:02d}:00"


def _time_str_to_minutes(t_str: str) -> Optional[int]:
    """Convert HH:MM[:SS] string to minutes from midnight."""
    normalized = _normalize_time_to_hhmmss(t_str)
    if not normalized:
        return None
    try:
        parts = normalized.split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except (TypeError, ValueError, IndexError):
        return None


def normalize_time_for_slot(h: int, m: int) -> Tuple[int, int]:
    """Floor to 10-minute boundary for matching DB (same as clientAvailabilityMigration)."""
    total_min = h * 60 + m
    floored = (total_min // 10) * 10
    return floored // 60, floored % 60


def _get_day_of_week(dt) -> str:
    """Get day of week name from date (0=Monday, 6=Sunday)."""
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    if hasattr(dt, 'weekday'):
        return days[dt.weekday()]
    return days[dt]


def _normalize_time_for_match(t_str: str) -> str:
    """Normalize time string to HH:MM for matching (10-minute tolerance)."""
    if not t_str:
        return "00:00"
    parts = t_str.split(':')
    hour = int(parts[0]) if len(parts) > 0 else 0
    minute = int(parts[1]) if len(parts) > 1 else 0
    # Round to nearest 10 minutes
    minute = (minute // 10) * 10
    return f"{hour:02d}:{minute:02d}"


def _occurrence_is_covered(
    occurrence_date,  # date object or datetime
    occurrence_start_time: str,  # HH:MM:SS or HH:MM
    occurrence_end_time: str,    # HH:MM:SS or HH:MM
    avail_record: Dict
) -> bool:
    """
    Check if a specific CSV visit (occurrence) matches a client_availability pattern.

    For clientWindowsAnalyzer, we match visits to availability patterns regardless of
    start_date, because we're analyzing historical data to SET the windows, not
    checking if visits fall within a scheduled period.

    A record matches an occurrence if:
    1. The day of week matches
    2. The time range matches (allowing 10-minute tolerance)
    3. The recurrence pattern matches (weekly/bi-weekly)

    Parameters:
    - occurrence_date: The specific date to check (date or datetime object)
    - occurrence_start_time: Start time string "HH:MM:SS" or "HH:MM"
    - occurrence_end_time: End time string "HH:MM:SS" or "HH:MM"
    - avail_record: Database record with fields:
        - days: list of day names
        - requested_start_time: "HH:MM:SS"
        - requested_end_time: "HH:MM:SS"
        - start_date: date object (used as reference for recurrence calculation)
        - occurs_every: int (1=weekly, 2=bi-weekly, etc.)
    """
    if hasattr(occurrence_date, 'date'):
        occurrence_date = occurrence_date.date()

    occurrence_dow = _get_day_of_week(occurrence_date)

    # Check day of week
    if occurrence_dow not in avail_record.get('days', []):
        return False

    # Check time match (10-minute tolerance)
    occ_start_norm = _normalize_time_for_match(occurrence_start_time)
    occ_end_norm = _normalize_time_for_match(occurrence_end_time)

    db_start_norm = _normalize_time_for_match(avail_record.get('requested_start_time', ''))
    db_end_norm = _normalize_time_for_match(avail_record.get('requested_end_time', ''))

    if db_start_norm != occ_start_norm or db_end_norm != occ_end_norm:
        return False

    # Check recurrence pattern (weekly/bi-weekly)
    # For clientWindowsAnalyzer, we don't enforce start_date - we want to match
    # ALL historical visits that fit this pattern, even if they're before start_date
    record_start_date = avail_record.get('start_date')
    occurs_every = avail_record.get('occurs_every', 1)

    # If occurs_every = 1 (weekly), all matching day/time visits belong to this pattern
    if occurs_every == 1:
        return True

    # For bi-weekly patterns, we need to check if this date aligns with the pattern
    # Use start_date as a reference point to determine which week this is
    if not record_start_date:
        # If no start_date, can't determine bi-weekly alignment, assume it matches
        return True

    # Calculate which week this occurrence falls into relative to start_date
    # Works for dates both before and after start_date
    occurrence_dow_index = occurrence_date.weekday()

    # Find a reference date with the same day of week as the occurrence
    start_dow_index = record_start_date.weekday()
    if occurrence_dow_index >= start_dow_index:
        days_to_first = occurrence_dow_index - start_dow_index
    else:
        days_to_first = 7 - start_dow_index + occurrence_dow_index

    from datetime import timedelta
    reference_date = record_start_date + timedelta(days=days_to_first)

    # Calculate weeks difference (can be negative for dates before start_date)
    days_diff = (occurrence_date - reference_date).days
    weeks_diff = days_diff // 7

    # For bi-weekly: check if this week aligns with the pattern
    # weeks_diff % 2 == 0 means even weeks (0, 2, 4, -2, -4, etc.)
    if weeks_diff % occurs_every == 0:
        return True

    return False


# ============================================================================
# STAGE 1 — Load & Clean
# ============================================================================

def stage1_load_and_clean(csv_path: str) -> pd.DataFrame:
    """
    Read CSV, filter Personal Care, parse datetimes, keep latest per (patient, Service Requirement Start),
    drop missing Actual Start/End, duration >= 20 min. No date range filter.
    """
    logger.info("Stage 1: Load & Clean (Service Requirement Duration >= %s min)", MINIMUM_DURATION_MINUTES)
    path = Path(csv_path)
    if not path.exists():
        raise MigrationError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    # Normalize column names (strip spaces)
    df.columns = [str(c).strip() for c in df.columns]

    # Filter: Service Requirement Service Type Description == 'Personal Care'
    col_svc_type = "Service Requirement Service Type Description"
    if col_svc_type not in df.columns:
        raise MigrationError(f"Missing column: {col_svc_type}. Columns: {list(df.columns)}")
    df = df[df[col_svc_type].astype(str).str.strip().str.lower() == "personal care"].copy()
    logger.info(f"  After Personal Care filter: {len(df)} rows")

    # Parse datetimes
    cols_dt = [
        "Service Requirement Start Date And Time",
        "Service Requirement End Date And Time",
        "Actual Start Date And Time",
        "Actual End Date And Time",
        "Service Location Updated Date & Time",
    ]
    for c in cols_dt:
        if c not in df.columns:
            continue
        df[c] = df[c].apply(parse_flexible_datetime)

    # Standardize patient names: "Lastname, Firstname" -> strip and lowercase key
    col_loc = "Service Location Name"
    if col_loc not in df.columns:
        raise MigrationError(f"Missing column: {col_loc}")
    # Fix encoding (e.g. O‚ÄôCeallaigh -> O'Ceallaigh) so matching to DB works
    df["Formatted_Name"] = df[col_loc].astype(str).apply(lambda x: fix_utf8_mojibake(x).strip())

    # Keep latest per (patient, Service Requirement Start Date And Time) by Service Location Updated Date & Time desc
    col_req_start = "Service Requirement Start Date And Time"
    col_updated = "Service Location Updated Date & Time"
    if col_updated in df.columns:
        df = df.sort_values(col_updated, ascending=False)
    df = df.drop_duplicates(subset=[col_loc, col_req_start], keep="first")
    logger.info(f"  After dedupe (latest per patient, req start): {len(df)} rows")

    # Day of week for Stage 3 (0=Monday, 6=Sunday - match Python weekday)
    df["day_of_week"] = df[col_req_start].dt.weekday

    # Drop rows missing Actual Start/End
    col_act_start = "Actual Start Date And Time"
    col_act_end = "Actual End Date And Time"
    before_act = len(df)
    df = df[df[col_act_start].notna() & df[col_act_end].notna()]
    dropped_act = before_act - len(df)
    logger.info(f"  After dropping missing Actual Start/End: {len(df)} rows (dropped {dropped_act} with missing Actual Start/End)")

    # Calculate minute-from-midnight values for actual times (needed for matching later)
    df["Actual_start_min"] = df[col_act_start].dt.hour * 60 + df[col_act_start].dt.minute
    df["Actual_end_min"] = df[col_act_end].dt.hour * 60 + df[col_act_end].dt.minute

    # Drop rows where actual time calculation failed (resulted in NaN)
    before_time_calc = len(df)
    df = df[df["Actual_start_min"].notna() & df["Actual_end_min"].notna()]
    dropped_time_calc = before_time_calc - len(df)
    if dropped_time_calc > 0:
        logger.info(f"  After dropping rows with invalid actual times: {len(df)} rows (dropped {dropped_time_calc})")

    # Duration: CSV stores HOURS (e.g. 0.75 = 45 min) — convert to minutes like Patient_Analyzer_1.py
    col_req_dur = "Service Requirement Duration"
    col_act_dur = "Actual Duration"
    if col_req_dur in df.columns:
        before_dur = len(df)
        # VisitExport.csv has duration in hours; convert to minutes
        raw_req = pd.to_numeric(df[col_req_dur], errors="coerce")
        df[col_req_dur] = (raw_req * 60).fillna(0).astype(int)
        nan_req = (raw_req.isna() | (raw_req <= 0)).sum()
        below_20 = (df[col_req_dur] < MINIMUM_DURATION_MINUTES).sum()
        df = df[(df[col_req_dur] >= MINIMUM_DURATION_MINUTES)]
        logger.info(f"  After Service Requirement Duration >= {MINIMUM_DURATION_MINUTES} min (CSV hours→min): {len(df)} rows (dropped {nan_req} NaN/zero, {below_20} < {MINIMUM_DURATION_MINUTES} min)")
    else:
        logger.warning(f"  Column '{col_req_dur}' not found; skipping duration filter. Columns: {list(df.columns)[:15]}...")
    if col_act_dur in df.columns:
        before_act_dur = len(df)
        # Actual Duration in CSV is also in hours
        raw_act = pd.to_numeric(df[col_act_dur], errors="coerce")
        df[col_act_dur] = (raw_act * 60).fillna(0).astype(int)
        nan_act_dur = (raw_act.isna() | (raw_act <= 0)).sum()
        df = df[df[col_act_dur].notna() & (df[col_act_dur] > 0)]
        logger.info(f"  After dropping missing/zero Actual Duration (hours→min): {len(df)} rows (dropped {nan_act_dur} NaN/zero)")
    else:
        logger.warning(f"  Column '{col_act_dur}' not found; computing from Actual Start/End. Columns: {list(df.columns)[:15]}...")

    logger.info(f"  Stage 1 output: {len(df)} rows → {df['Formatted_Name'].nunique() if len(df) else 0} unique clients")
    df = apply_productivity_scaling_algorithm(df)
    return df


def apply_productivity_scaling_algorithm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply duration scaling logic inspired by duration_reduction_visitexport.py:
    per carer/day route, scale actual visit durations by:
        scaling_factor = (shift_window - waiting_gaps) / total_reported_care
    clamped to [0, 1].
    Travel time is treated as 0 here because this migration does not load distance matrices.
    """
    col_carer = "Actual Employee Name"
    col_act_start = "Actual Start Date And Time"

    out = df.copy()
    out["Adjusted Duration Estimate"] = np.nan

    if col_carer not in out.columns:
        logger.warning("  Column '%s' not found; scaling durations without carer grouping.", col_carer)
        out[col_carer] = "__UNKNOWN_CARER__"

    out["_route_date"] = out[col_act_start].dt.date
    groups = out.groupby([col_carer, "_route_date"], dropna=False)

    scaled_routes = 0
    for (_, _), grp in groups:
        if grp.empty:
            continue

        grp_sorted = grp.sort_values(col_act_start)
        total_waiting_gaps = 0
        total_reported_care_time = 0
        first_start = None
        last_end = None
        prev_end = None
        calc_duration_by_idx: Dict[int, int] = {}

        for idx, row in grp_sorted.iterrows():
            start_min = int(row["Actual_start_min"])
            end_min = int(row["Actual_end_min"])
            if end_min < start_min:
                end_min += 24 * 60

            calc_duration = max(0, end_min - start_min)
            calc_duration_by_idx[idx] = calc_duration
            total_reported_care_time += calc_duration

            if first_start is None:
                first_start = start_min
            last_end = end_min

            if prev_end is not None:
                gap = start_min - prev_end
                if gap > 0:
                    total_waiting_gaps += gap
            prev_end = end_min

        shift_window = (last_end - first_start) if first_start is not None and last_end is not None else 0
        available_time = shift_window - total_waiting_gaps
        scaling_factor = (available_time / total_reported_care_time) if total_reported_care_time > 0 else 1
        scaling_factor = max(0, min(1, scaling_factor))

        for idx, calc_duration in calc_duration_by_idx.items():
            out.at[idx, "Adjusted Duration Estimate"] = int(round(calc_duration * scaling_factor))

        scaled_routes += 1

    out.drop(columns=["_route_date"], inplace=True)
    logger.info("  Applied productivity scaling to %d carer/day routes", scaled_routes)
    return out


# ============================================================================
# STAGE 2 — Initial Pattern Intelligence
# ============================================================================

def stage2_initial_pattern_intelligence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by (Formatted_Name, req_start_hour, req_start_minute, req_end_hour, req_end_minute, Service Requirement Duration).
    Per group: sugg_start_min = 10th percentile(Actual_start_min) - TOLERANCE_MINS,
               sugg_end_min = 90th percentile(Actual_end_min) + TOLERANCE_MINS,
               min window width 60, minDuration = min(Actual Duration).
    """
    logger.info("Stage 2: Initial Pattern Intelligence")

    col_req_start = "Service Requirement Start Date And Time"
    col_req_end = "Service Requirement End Date And Time"
    col_act_start = "Actual Start Date And Time"
    col_act_end = "Actual End Date And Time"
    col_req_dur = "Service Requirement Duration"
    col_act_dur = "Actual Duration"

    df = df.copy()
    df["req_start_hour"] = df[col_req_start].dt.hour
    df["req_start_minute"] = df[col_req_start].dt.minute
    df["req_end_hour"] = df[col_req_end].dt.hour
    df["req_end_minute"] = df[col_req_end].dt.minute
    df["Actual_start_min"] = df[col_act_start].dt.hour * 60 + df[col_act_start].dt.minute
    df["Actual_end_min"] = df[col_act_end].dt.hour * 60 + df[col_act_end].dt.minute

    if col_act_dur not in df.columns:
        df[col_act_dur] = (df[col_act_end] - df[col_act_start]).dt.total_seconds() / 60

    grp_cols = ["Formatted_Name", "day_of_week", "req_start_hour", "req_start_minute", "req_end_hour", "req_end_minute"]
    if col_req_dur in df.columns:
        grp_cols.append(col_req_dur)
    else:
        df[col_req_dur] = 0

    def agg_func(g):
        start_mins = g["Actual_start_min"].values
        end_mins = g["Actual_end_min"].values
        if len(start_mins) == 1:
            sugg_start = start_mins[0] - TOLERANCE_MINS
            sugg_end = end_mins[0] + TOLERANCE_MINS
        else:
            sugg_start = np.percentile(start_mins, LOWER_PERCENTILE * 100) - TOLERANCE_MINS
            sugg_end = np.percentile(end_mins, UPPER_PERCENTILE * 100) + TOLERANCE_MINS
        width = sugg_end - sugg_start
        if width < MIN_WINDOW_WIDTH_MINS:
            extra = (MIN_WINDOW_WIDTH_MINS - width) / 2
            sugg_start -= extra
            sugg_end += extra
        sugg_start = max(0, int(round(sugg_start)))
        sugg_end = min(24 * 60 - 1, int(round(sugg_end)))
        min_dur = int(g[col_act_dur].min()) if g[col_act_dur].notna().any() else None
        return pd.Series({
            "sugg_start_min": sugg_start,
            "sugg_end_min": sugg_end,
            "minDuration": min_dur,
        })

    try:
        stage2 = df.groupby(grp_cols, dropna=False).apply(agg_func, include_groups=False).reset_index()
    except TypeError:
        # pandas < 2.2 does not have include_groups
        stage2 = df.groupby(grp_cols, dropna=False).apply(agg_func).reset_index()
    logger.info(f"  Stage 2 patterns: {len(stage2)} (unique client+slot combinations)")
    return stage2


# ============================================================================
# STAGE 3 — Context-Aware Suggestion Engine
# ============================================================================

def _clamp_suggested_window_to_required(
    sugg_start_min: int,
    sugg_end_min: int,
    req_start_min: int,
    req_end_min: int,
) -> Tuple[int, int]:
    """
    Clamp suggested window to [req_start_min - TOLERANCE_MINS, req_end_min + TOLERANCE_MINS].
    If clamping makes the window narrower than MIN_WINDOW_WIDTH_MINS, center within required
    boundaries and expand to minimum width while respecting required range as hard boundary.
    """
    lo = max(sugg_start_min, req_start_min - TOLERANCE_MINS)
    hi = min(sugg_end_min, req_end_min + TOLERANCE_MINS)
    if hi - lo >= MIN_WINDOW_WIDTH_MINS:
        return lo, hi
    allowed_width = req_end_min - req_start_min
    if allowed_width < MIN_WINDOW_WIDTH_MINS:
        return req_start_min, req_end_min
    center = (req_start_min + req_end_min) / 2
    half = MIN_WINDOW_WIDTH_MINS / 2
    lo = max(req_start_min, int(center - half))
    hi = min(req_end_min, int(center + half))
    if hi - lo < MIN_WINDOW_WIDTH_MINS:
        lo = req_start_min
        hi = req_end_min
    return lo, hi


def stage3_context_aware_suggestion(stage2: pd.DataFrame) -> pd.DataFrame:
    """
    Per patient, per day of week: find all patterns, sort by required start.
    Sub-group contiguous (current start = previous end) or concurrent (same start).
    Apply conflict resolution (pinch inward, only between different groups). Clamp suggested
    windows to required boundaries. Aggregate with median; clamp again.
    """
    logger.info("Stage 3: Context-Aware Suggestion Engine")

    # Per (Formatted_Name, day_of_week) get list of patterns with req_start_min, req_end_min, sugg_start_min, sugg_end_min
    stage2["req_start_min"] = stage2["req_start_hour"] * 60 + stage2["req_start_minute"]
    stage2["req_end_min"] = stage2["req_end_hour"] * 60 + stage2["req_end_minute"]

    rows_out = []
    for (name, day), grp in stage2.groupby(["Formatted_Name", "day_of_week"]):
        patterns = grp.sort_values("req_start_min").to_dict("records")
        if not patterns:
            continue
        # Single pattern: clamp and use
        if len(patterns) == 1:
            p = patterns[0]
            sugg_start, sugg_end = _clamp_suggested_window_to_required(
                int(p["sugg_start_min"]), int(p["sugg_end_min"]),
                int(p["req_start_min"]), int(p["req_end_min"]),
            )
            rows_out.append({
                "Formatted_Name": name,
                "day_of_week": day,
                "req_start_hour": p["req_start_hour"],
                "req_start_minute": p["req_start_minute"],
                "req_end_hour": p["req_end_hour"],
                "req_end_minute": p["req_end_minute"],
                "sugg_start_min": sugg_start,
                "sugg_end_min": sugg_end,
                "minDuration": p["minDuration"],
                "Service Requirement Duration": p.get("Service Requirement Duration"),
            })
            continue
        # Multiple: group into contiguous/concurrent and assign group_index
        adjusted = []
        group_idx = 0
        i = 0
        while i < len(patterns):
            p = patterns[i]
            s_start = p["sugg_start_min"]
            s_end = p["sugg_end_min"]
            # Look for concurrent (same req_start_min)
            j = i + 1
            while j < len(patterns) and patterns[j]["req_start_min"] == p["req_start_min"]:
                j += 1
            if j > i + 1:
                # Concurrent: same wide window; all share group_index
                group = patterns[i:j]
                s_start = min(px["sugg_start_min"] for px in group)
                s_end = max(px["sugg_end_min"] for px in group)
                for px in group:
                    adjusted.append({
                        **px,
                        "sugg_start_min": s_start,
                        "sugg_end_min": s_end,
                        "group_index": group_idx,
                    })
                group_idx += 1
                i = j
                continue
            # Single pattern (or contiguous); own group
            adjusted.append({**p, "group_index": group_idx})
            group_idx += 1
            i += 1
        # Inter-group conflict: only between different group_index; pinch inward (prev end down, curr start up; curr end unchanged)
        for k in range(len(adjusted)):
            if k == 0:
                continue
            prev = adjusted[k - 1]
            curr = adjusted[k]
            if prev["group_index"] == curr["group_index"]:
                continue
            overlap = prev["sugg_end_min"] - curr["sugg_start_min"]
            if overlap > 0:
                shift = (overlap / 2) + 5
                prev["sugg_end_min"] = prev["sugg_end_min"] - shift
                curr["sugg_start_min"] = curr["sugg_start_min"] + shift
                # curr["sugg_end_min"] stays unchanged
        # Clamp each pattern to required boundaries before appending
        for p in adjusted:
            sugg_start, sugg_end = _clamp_suggested_window_to_required(
                int(p["sugg_start_min"]), int(p["sugg_end_min"]),
                int(p["req_start_min"]), int(p["req_end_min"]),
            )
            rows_out.append({
                "Formatted_Name": name,
                "day_of_week": day,
                "req_start_hour": p["req_start_hour"],
                "req_start_minute": p["req_start_minute"],
                "req_end_hour": p["req_end_hour"],
                "req_end_minute": p["req_end_minute"],
                "sugg_start_min": sugg_start,
                "sugg_end_min": sugg_end,
                "minDuration": p["minDuration"],
                "Service Requirement Duration": p.get("Service Requirement Duration"),
            })

    out = pd.DataFrame(rows_out)
    if out.empty:
        out = stage2.copy()
        out["requested_start_str"] = out.apply(
            lambda r: min_to_time_str(int(r["req_start_hour"]) * 60 + int(r["req_start_minute"])), axis=1
        )
        out["requested_end_str"] = out.apply(
            lambda r: min_to_time_str(int(r["req_end_hour"]) * 60 + int(r["req_end_minute"])), axis=1
        )
        out["start_time_str"] = out["sugg_start_min"].apply(min_to_time_str)
        out["end_time_str"] = out["sugg_end_min"].apply(min_to_time_str)
        return out

    # Aggregate by (Formatted_Name, req_start, req_end) across days: median(sugg_start), median(sugg_end), min(minDuration)
    out["req_start_min"] = out["req_start_hour"] * 60 + out["req_start_minute"]
    out["req_end_min"] = out["req_end_hour"] * 60 + out["req_end_minute"]
    agg = out.groupby(["Formatted_Name", "req_start_hour", "req_start_minute", "req_end_hour", "req_end_minute"]).agg({
        "sugg_start_min": lambda x: int(np.floor(np.median(x))),
        "sugg_end_min": lambda x: int(np.ceil(np.median(x))),
        "minDuration": "min",
        "Service Requirement Duration": "first",
    }).reset_index()
    # Boundary clamp after aggregation (safety net)
    def clamp_agg_row(r):
        req_start = int(r["req_start_hour"]) * 60 + int(r["req_start_minute"])
        req_end = int(r["req_end_hour"]) * 60 + int(r["req_end_minute"])
        return _clamp_suggested_window_to_required(
            int(r["sugg_start_min"]), int(r["sugg_end_min"]), req_start, req_end,
        )

    sugg_pairs = agg.apply(clamp_agg_row, axis=1)
    agg["sugg_start_min"] = [p[0] for p in sugg_pairs]
    agg["sugg_end_min"] = [p[1] for p in sugg_pairs]
    agg["requested_start_str"] = agg.apply(
        lambda r: min_to_time_str(int(r["req_start_hour"]) * 60 + int(r["req_start_minute"])), axis=1
    )
    agg["requested_end_str"] = agg.apply(
        lambda r: min_to_time_str(int(r["req_end_hour"]) * 60 + int(r["req_end_minute"])), axis=1
    )
    agg["start_time_str"] = agg["sugg_start_min"].apply(min_to_time_str)
    agg["end_time_str"] = agg["sugg_end_min"].apply(min_to_time_str)
    return agg


# ============================================================================
# STAGE 3.5 — Routine Anomaly Detection & Removal
# ============================================================================

def stage3_5_remove_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group patterns by (Formatted_Name, req_start_hour, req_start_minute). For each group
    with 2+ entries, find the most common Service Requirement Duration (standard) and
    drop rows that differ (anomalies). Log each anomaly and a summary count.
    """
    logger.info("Stage 3.5: Routine Anomaly Detection & Removal")
    col_dur = "Service Requirement Duration"
    if col_dur not in df.columns:
        logger.warning("  Column '%s' not found; skipping anomaly removal.", col_dur)
        return df

    total_before = len(df)
    anchor_cols = ["Formatted_Name", "req_start_hour", "req_start_minute"]
    # Build standard duration per anchor: mode of Service Requirement Duration
    mode_per_anchor = df.groupby(anchor_cols)[col_dur].agg(
        lambda s: s.mode().iloc[0] if len(s) and len(s.mode()) else s.iloc[0]
    ).to_dict()

    anomaly_indices = []
    for (name, h, m), grp in df.groupby(anchor_cols):
        if len(grp) < 2:
            continue
        standard = mode_per_anchor.get((name, h, m))
        if standard is None:
            continue
        for idx, row in grp.iterrows():
            dur = row[col_dur]
            if pd.isna(dur) or int(dur) != int(standard):
                anomaly_indices.append(idx)
                slot = f"{int(h):02d}:{int(m):02d}"
                req_end = row.get("requested_end_str", "")
                if not req_end and "req_end_hour" in row.index and "req_end_minute" in row.index:
                    req_end = f"{int(row['req_end_hour']):02d}:{int(row['req_end_minute']):02d}"
                extra = f" req_end=%s" % req_end if req_end else ""
                logger.info(
                    "  Anomaly: patient=%s time_slot=%s standard_duration=%s min anomaly_duration=%s min%s",
                    name, slot, int(standard), int(dur) if not pd.isna(dur) else "NaN", extra,
                )

    out = df.drop(index=anomaly_indices).reset_index(drop=True)
    removed = len(anomaly_indices)
    logger.info("  Removed %d anomalous patterns out of %d total", removed, total_before)
    return out


# ============================================================================
# STAGE 3.7 — Suggested Duration Refinement
# ============================================================================

def stage3_7_refine_duration(pattern_df: pd.DataFrame, stage1_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each pattern, use actual duration distribution from Stage 1. Build frequency map
    for durations <= Service Requirement Duration; apply 10%% significance threshold;
    set suggested_duration to highest significant duration < required, else keep required.
    Set minDuration = suggested_duration for DB update.
    """
    logger.info("Stage 3.7: Suggested Duration Refinement")
    col_req_start = "Service Requirement Start Date And Time"
    col_req_end = "Service Requirement End Date And Time"
    col_req_dur = "Service Requirement Duration"
    col_act_dur = "Actual Duration"

    if col_act_dur not in stage1_df.columns or col_req_dur not in pattern_df.columns:
        logger.warning("  Missing duration columns; skipping refinement.")
        return pattern_df

    # Add req hour/minute to Stage 1 for matching
    s1 = stage1_df.copy()
    s1["req_start_hour"] = s1[col_req_start].dt.hour
    s1["req_start_minute"] = s1[col_req_start].dt.minute
    s1["req_end_hour"] = s1[col_req_end].dt.hour
    s1["req_end_minute"] = s1[col_req_end].dt.minute

    match_cols = ["Formatted_Name", "req_start_hour", "req_start_minute", "req_end_hour", "req_end_minute"]
    suggested = []
    for _, row in pattern_df.iterrows():
        req_dur = row[col_req_dur]
        if pd.isna(req_dur):
            suggested.append(req_dur)
            continue
        req_dur = int(req_dur)
        mask = True
        for c in match_cols:
            if c not in s1.columns or c not in row.index:
                mask = pd.Series(True, index=s1.index)
                break
            mask = mask & (s1[c] == row[c])
        subset = s1.loc[mask]
        if subset.empty:
            suggested.append(req_dur)
            continue
        # Frequency map: duration -> count, only durations <= req_dur
        dur_counts: Dict[int, int] = defaultdict(int)
        for d in subset[col_act_dur].dropna():
            d = int(d)
            if d <= req_dur:
                dur_counts[d] += 1
        total = subset.shape[0]
        if total == 0:
            suggested.append(req_dur)
            continue
        threshold_count = max(1, int(round(total * DURATION_SIGNIFICANCE_THRESHOLD)))
        significant = [d for d, cnt in dur_counts.items() if cnt >= threshold_count and d < req_dur]
        if significant:
            chosen = max(significant)
            suggested.append(chosen)
        else:
            suggested.append(req_dur)

    out = pattern_df.copy()
    out["suggested_duration"] = suggested

    # Ensure minDuration is not bigger than duration (Service Requirement Duration)
    def clamp_min_duration(row):
        min_dur = row["suggested_duration"]
        req_dur = row.get(col_req_dur)
        if pd.isna(min_dur) or pd.isna(req_dur):
            return min_dur
        min_dur = int(min_dur)
        req_dur = int(req_dur)
        if min_dur > req_dur:
            logger.warning(f"  Clamping minDuration for {row.get('Formatted_Name')}: {min_dur} min > required {req_dur} min, setting to {req_dur}")
            return req_dur
        return min_dur

    out["minDuration"] = out.apply(clamp_min_duration, axis=1)
    logger.info("  Refined minDuration for %d patterns using %.0f%% significance threshold",
                len(out), DURATION_SIGNIFICANCE_THRESHOLD * 100)
    return out


# ============================================================================
# MATCH AND UPDATE
# ============================================================================

def build_availability_lookup(availabilities: List[Dict]) -> Dict[Tuple[int, str, str], List[Dict]]:
    """Key: (client_id, requested_start_time, requested_end_time) -> list of records (same slot can exist per day)."""
    lookup = {}
    for a in availabilities:
        key = (a["client_id"], a["requested_start_time"], a["requested_end_time"])
        if key not in lookup:
            lookup[key] = []
        lookup[key].append(a)
    return lookup


def run(csv_path: Optional[str] = None, connection_manager=None, state=None) -> bool:
    """
    Entry point: run 3-stage analysis on CSV, then UPDATE client_availabilities.
    connection_manager and state used from wizard for resume support.
    """
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║   CLIENT WINDOWS ANALYZER                                      ║
    ║   Updates start_time, end_time, minDuration from visit CSV     ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    if state and state.is_completed("client_windows"):
        logger.info("Client windows migration already completed (resume).")
        return True
    if not csv_path or not Path(csv_path).exists():
        logger.error("CSV path is required and must exist: %s", csv_path)
        return False
    connection = None
    try:
        # Load and clean CSV data
        df1 = stage1_load_and_clean(csv_path)
        if df1.empty:
            logger.warning("No data after Stage 1; nothing to update.")
            return True

        logger.info(f"  Stage 1 complete: {len(df1)} CSV visit records loaded")

        # Connect to database
        config = get_db_config()
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
            connection = connect_to_database(config)

        clients_map = get_all_clients(connection)
        avail_list, skip_reasons = load_client_availabilities(connection)

        logger.info(f"  Loaded {len(avail_list)} client_availabilities records for analysis")
        logger.info(f"  Loaded {len(clients_map)} clients from database")

        # Build reverse lookup: client_id -> list of availability records
        client_avails = defaultdict(list)
        for avail in avail_list:
            client_avails[avail["client_id"]].append(avail)

        # Build CSV lookup: client name -> list of visit records (with dates)
        csv_by_client = defaultdict(list)
        for _, row in df1.iterrows():
            raw_name = (row.get("Formatted_Name") or "").strip()
            name_key = normalize_name_for_match(raw_name) if raw_name else ""
            if not name_key:
                continue
            client_id = clients_map.get(name_key)
            if not client_id:
                continue

            # Extract visit information
            visit_date = row.get("Service Requirement Start Date And Time")
            if pd.isna(visit_date):
                continue

            visit_start_time = visit_date.strftime("%H:%M:%S")
            visit_end_datetime = row.get("Service Requirement End Date And Time")
            if pd.isna(visit_end_datetime):
                continue
            visit_end_time = visit_end_datetime.strftime("%H:%M:%S")

            actual_start_min = row.get("Actual_start_min")
            actual_end_min = row.get("Actual_end_min")
            actual_duration = row.get("Actual Duration")
            adjusted_duration = row.get("Adjusted Duration Estimate")

            csv_by_client[client_id].append({
                "date": visit_date,
                "start_time": visit_start_time,
                "end_time": visit_end_time,
                "actual_start_min": actual_start_min,
                "actual_end_min": actual_end_min,
                "actual_duration": actual_duration,
                "adjusted_duration": adjusted_duration,
            })

        logger.info(f"  CSV visits grouped by {len(csv_by_client)} clients")

        # ======================================================================
        # NEW MATCHING LOGIC: For each availability, find matching CSV visits
        # ======================================================================
        updated = 0
        updated_ids = set()
        update_reasons = {}
        update_args = []

        for avail in avail_list:
            client_id = avail["client_id"]
            avail_id = avail["id"]

            # Get all CSV visits for this client
            visits = csv_by_client.get(client_id, [])
            if not visits:
                skip_reasons[avail_id] = "No CSV visits found for this client"
                continue

            # Find visits covered by this availability
            matching_visits = []
            for visit in visits:
                if _occurrence_is_covered(
                    visit["date"],
                    visit["start_time"],
                    visit["end_time"],
                    avail
                ):
                    matching_visits.append(visit)

            if not matching_visits:
                skip_reasons[avail_id] = "No CSV visits match this availability pattern (day/time/recurrence)"
                continue

            # Apply productivity-scaled duration algorithm to derive minDuration and window.
            # We intentionally keep DB duration/requested_start_time/requested_end_time untouched.
            adjusted_durations = [
                int(round(v["adjusted_duration"]))
                for v in matching_visits
                if not pd.isna(v.get("adjusted_duration"))
            ]
            if not adjusted_durations:
                skip_reasons[avail_id] = (
                    f"Found {len(matching_visits)} matching visits but no adjusted durations from scaling algorithm"
                )
                continue

            req_start_min = _time_str_to_minutes(avail.get("requested_start_time"))
            req_end_min = _time_str_to_minutes(avail.get("requested_end_time"))
            if req_start_min is None or req_end_min is None or req_end_min <= req_start_min:
                skip_reasons[avail_id] = "Invalid requested start/end time; cannot derive window"
                continue

            requested_window = req_end_min - req_start_min
            min_duration = int(round(np.median(adjusted_durations)))

            db_duration = avail.get("duration")
            if db_duration is not None and not pd.isna(db_duration):
                min_duration = min(min_duration, int(db_duration))
            min_duration = min(min_duration, requested_window)

            if min_duration <= 0:
                skip_reasons[avail_id] = "Scaled duration collapsed to 0; skipping update"
                continue

            start_time_str = _normalize_time_to_hhmmss(avail.get("requested_start_time"))
            end_time_str = min_to_time_str(req_start_min + min_duration)

            # Add to update batch
            update_args.append((start_time_str, end_time_str, min_duration, avail_id))
            updated_ids.add(avail_id)
            update_reasons[avail_id] = (
                f"Successfully updated from {len(matching_visits)} matching CSV visits "
                f"using scaled durations (start_time={start_time_str}, end_time={end_time_str}, minDuration={min_duration})"
            )
            updated += 1

        logger.info(f"  Matched {updated} availabilities to CSV visits")

        cursor = connection.cursor()
        try:
            if update_args:
                execute_batch(
                    cursor,
                    """UPDATE client_availabilities
                       SET start_time = %s, end_time = %s, "minDuration" = %s, last_modified_date = NOW()
                       WHERE id = %s""",
                    update_args,
                    page_size=500,
                )
            connection.commit()
        except Exception as e:
            connection.rollback()
            logger.exception("Update failed: %s", e)
            raise MigrationError(f"Update failed: {e}")
        finally:
            cursor.close()

        # ======================================================================
        # TRACK NON-UPDATED RECORDS
        # ======================================================================
        # For records that were analyzed but not updated, determine why
        for rec in avail_list:
            rec_id = rec["id"]
            if rec_id not in updated_ids and rec_id not in skip_reasons:
                # This record was eligible for analysis but no pattern matched
                skip_reasons[rec_id] = "No matching time slot pattern found in CSV analysis"

        # Reload ALL client_availabilities from DB to get complete data for report
        cursor = connection.cursor()
        try:
            cursor.execute("""
                SELECT ca.id, ca.client_id, c.name as client_name, c.lastname as client_lastname,
                       ca.requested_start_time, ca.requested_end_time, ca.days,
                       ca.number_of_care_givers, ca.start_time, ca.end_time, ca."minDuration"
                FROM client_availabilities ca
                LEFT JOIN client c ON ca.client_id = c.id
                WHERE ca.deleted_at IS NULL
                ORDER BY c.lastname, c.name, ca.requested_start_time
            """)
            all_availabilities = cursor.fetchall()
        finally:
            cursor.close()

        # ======================================================================
        # GENERATE COMPREHENSIVE REPORT
        # ======================================================================
        logger.info("")
        logger.info("=" * 80)
        logger.info("CLIENT AVAILABILITY DETAILED REPORT")
        logger.info("=" * 80)
        logger.info("This report shows each client_availability record and why its fields")
        logger.info("were filled (UPDATED) or not filled (SKIPPED).")
        logger.info("=" * 80)
        logger.info("")

        # Group by client for clearer reporting
        client_groups = defaultdict(list)
        for row in all_availabilities:
            client_id = row["client_id"]
            client_groups[client_id].append(row)

        # Report each client
        for client_id, records in sorted(client_groups.items()):
            first_rec = records[0]
            client_name = f"{first_rec['client_lastname'] or ''}, {first_rec['client_name'] or ''}".strip(', ')
            logger.info("")
            logger.info("-" * 80)
            logger.info(f"CLIENT: {client_name} (ID: {client_id})")
            logger.info("-" * 80)

            for rec in records:
                rec_id = rec["id"]
                req_start = _normalize_time_to_hhmmss(rec["requested_start_time"])
                req_end = _normalize_time_to_hhmmss(rec["requested_end_time"])

                # Parse days field properly
                days_raw = rec["days"]
                if isinstance(days_raw, str):
                    days = days_raw.strip('{}').split(',') if days_raw else []
                    days = [d.strip() for d in days if d.strip()]
                elif isinstance(days_raw, list):
                    days = days_raw
                else:
                    days = []

                num_caregivers = rec.get("number_of_care_givers", 1)

                # Get current values from DB
                start_time = _normalize_time_to_hhmmss(rec["start_time"]) if rec["start_time"] else "NULL"
                end_time = _normalize_time_to_hhmmss(rec["end_time"]) if rec["end_time"] else "NULL"
                min_duration = rec["minDuration"] if rec["minDuration"] is not None else "NULL"

                # Determine status and reason
                if rec_id in updated_ids:
                    status = "UPDATED"
                    reason = update_reasons.get(rec_id, "Updated from CSV pattern")
                else:
                    status = "SKIPPED"
                    reason = skip_reasons.get(rec_id, "Not analyzed (unknown reason)")

                logger.info(f"  Availability ID: {rec_id}")
                logger.info(f"    Requested Time:     {req_start} - {req_end}")
                logger.info(f"    Days:               {days}")
                logger.info(f"    Caregivers Needed:  {num_caregivers}")
                logger.info(f"    start_time:         {start_time}")
                logger.info(f"    end_time:           {end_time}")
                logger.info(f"    minDuration:        {min_duration}")
                logger.info(f"    Status:             {status}")
                logger.info(f"    Reason:             {reason}")
                logger.info("")

        # Summary report
        logger.info("")
        logger.info("=" * 60)
        logger.info("CLIENT WINDOWS ANALYZER SUMMARY")
        logger.info("=" * 60)
        logger.info("  Total client_availabilities in database: %d", len(avail_list) + len(skip_reasons))
        logger.info("  Analyzed (eligible for matching): %d", len(avail_list))
        logger.info("  Successfully updated: %d", updated)
        logger.info("  Skipped: %d", len(skip_reasons))
        logger.info("")
        logger.info("  Skip reasons breakdown:")

        # Count skip reasons by category
        reason_counts = defaultdict(int)
        for reason in skip_reasons.values():
            if "Multiple caregivers" in reason:
                reason_counts["Multiple caregivers required"] += 1
            elif "Overlapping time slots" in reason:
                reason_counts["Overlapping time slots"] += 1
            elif "No CSV visits found" in reason:
                reason_counts["No CSV visits for client"] += 1
            elif "No CSV visits match" in reason:
                reason_counts["No matching visits (day/time/recurrence mismatch)"] += 1
            elif "missing actual time data" in reason:
                reason_counts["Missing actual time data in CSV"] += 1
            else:
                reason_counts["Other"] += 1

        for reason, count in sorted(reason_counts.items()):
            logger.info("    - %s: %d", reason, count)

        logger.info("")
        logger.info("  Matching logic:")
        logger.info("    - CSV visits matched to availabilities using:")
        logger.info("      1. Client ID")
        logger.info("      2. Day of week (from availability.days array)")
        logger.info("      3. Time slot (with 10-minute tolerance)")
        logger.info("      4. Date falls within recurrence pattern (start_date + occurs_every)")
        logger.info("")
        logger.info("  Records excluded from analysis:")
        logger.info("    - number_of_care_givers >= 2 (multiple caregivers needed)")
        logger.info("    - Overlapping time slots on same day for same client")
        logger.info("")
        logger.info("  Additional filtering:")
        logger.info("    - CSV visits with Service Requirement Duration < %s min excluded", MINIMUM_DURATION_MINUTES)
        logger.info("=" * 60)

        if state:
            state.clear_step("client_windows")
        return True
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        if ConnectionLostError:
            raise ConnectionLostError("client_windows", {}) from e
        raise
    except MigrationError as e:
        logger.error("%s", e)
        return False
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        return False
    finally:
        if connection and not connection_manager:
            try:
                connection.close()
            except Exception:
                pass


if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    success = run(csv_path=csv_path)
    sys.exit(0 if success else 1)
