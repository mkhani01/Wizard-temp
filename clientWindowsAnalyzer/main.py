"""
Client Windows Analyzer
Updates existing client_availabilities records with optimized start_time, end_time, and minDuration
computed from historical visit data (VisitExport-style CSV) using a 3-stage analysis pipeline.
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
    print(f"Missing required package: {e}")
    print("Please install: pip install psycopg2-binary pandas numpy")
    sys.exit(1)

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIMUM_DURATION_MINUTES = 20
LOWER_PERCENTILE = 0.10
UPPER_PERCENTILE = 0.90
MIN_WINDOW_WIDTH_MINS = 60
TOLERANCE_MINS = 15
FLEXIBILITY_THRESHOLD = 10

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
            key = _normalize_client_key(lastname, name)
            if key:
                clients[key] = row["id"]
        logger.info(f"✓ Loaded {len(clients)} clients from database")
        return clients
    finally:
        cursor.close()


def _normalize_client_key(lastname: str, name: str) -> str:
    """Build lookup key: 'lastname, name' lowercased with single space after comma."""
    key = f"{lastname}, {name}".strip().lower()
    key = re.sub(r"\s+", " ", key)  # collapse spaces so "Hawkshaw (DS),  Harry" still matches
    return key


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


def load_client_availabilities(connection) -> List[Dict]:
    """Load existing client_availabilities: id, client_id, requested_start_time, requested_end_time.
    Multiple rows can share (client_id, requested_start_time, requested_end_time) (e.g. one per day).
    """
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT id, client_id, requested_start_time, requested_end_time
            FROM client_availabilities
            WHERE deleted_at IS NULL
        """)
        rows = cursor.fetchall()
        out = []
        for r in rows:
            rs = _normalize_time_to_hhmmss(r["requested_start_time"])
            re_ = _normalize_time_to_hhmmss(r["requested_end_time"])
            out.append({
                "id": r["id"],
                "client_id": r["client_id"],
                "requested_start_time": rs,
                "requested_end_time": re_,
            })
        logger.info(f"✓ Loaded {len(out)} client_availabilities records")
        return out
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


def normalize_time_for_slot(h: int, m: int) -> Tuple[int, int]:
    """Floor to 10-minute boundary for matching DB (same as clientAvailabilityMigration)."""
    total_min = h * 60 + m
    floored = (total_min // 10) * 10
    return floored // 60, floored % 60


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
    df["Formatted_Name"] = df[col_loc].astype(str).str.strip()

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
    return df


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

def stage3_context_aware_suggestion(stage2: pd.DataFrame) -> pd.DataFrame:
    """
    Per patient, per day of week: find all patterns, sort by required start.
    Sub-group contiguous (current start = previous end) or concurrent (same start).
    Apply conflict resolution. Then aggregate by (Formatted_Name, req_start, req_end) for DB matching.
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
        # Single pattern: use as-is
        if len(patterns) == 1:
            p = patterns[0]
            rows_out.append({
                "Formatted_Name": name,
                "day_of_week": day,
                "req_start_hour": p["req_start_hour"],
                "req_start_minute": p["req_start_minute"],
                "req_end_hour": p["req_end_hour"],
                "req_end_minute": p["req_end_minute"],
                "sugg_start_min": p["sugg_start_min"],
                "sugg_end_min": p["sugg_end_min"],
                "minDuration": p["minDuration"],
                "Service Requirement Duration": p.get("Service Requirement Duration"),
            })
            continue
        # Multiple: group into contiguous (current start = previous end) or concurrent (same start)
        adjusted = []
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
                # Concurrent: same wide window (min of sugg_start, max of sugg_end in group)
                group = patterns[i:j]
                s_start = min(px["sugg_start_min"] for px in group)
                s_end = max(px["sugg_end_min"] for px in group)
                for px in group:
                    adjusted.append({**px, "sugg_start_min": s_start, "sugg_end_min": s_end})
                i = j
                continue
            # Contiguous: check if this start equals previous end
            if adjusted and adjusted[-1]["sugg_end_min"] == p["req_start_min"]:
                # Stack back-to-back: keep suggested window
                adjusted.append(p)
            else:
                adjusted.append(p)
            i += 1
        # Inter-group conflict: shift overlapping by (overlap/2) + 5
        for k in range(len(adjusted)):
            if k == 0:
                continue
            prev = adjusted[k - 1]
            curr = adjusted[k]
            overlap = prev["sugg_end_min"] - curr["sugg_start_min"]
            if overlap > 0:
                shift = (overlap / 2) + 5
                curr["sugg_start_min"] = curr["sugg_start_min"] + shift
                curr["sugg_end_min"] = curr["sugg_end_min"] + shift
        for p in adjusted:
            rows_out.append({
                "Formatted_Name": name,
                "day_of_week": day,
                "req_start_hour": p["req_start_hour"],
                "req_start_minute": p["req_start_minute"],
                "req_end_hour": p["req_end_hour"],
                "req_end_minute": p["req_end_minute"],
                "sugg_start_min": p["sugg_start_min"],
                "sugg_end_min": p["sugg_end_min"],
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

    # Aggregate by (Formatted_Name, req_start, req_end) across days: min(sugg_start), max(sugg_end), min(minDuration)
    out["req_start_min"] = out["req_start_hour"] * 60 + out["req_start_minute"]
    out["req_end_min"] = out["req_end_hour"] * 60 + out["req_end_minute"]
    agg = out.groupby(["Formatted_Name", "req_start_hour", "req_start_minute", "req_end_hour", "req_end_minute"]).agg({
        "sugg_start_min": "min",
        "sugg_end_min": "max",
        "minDuration": "min",
    }).reset_index()
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


def run(csv_path: Optional[str] = None) -> bool:
    """
    Entry point: run 3-stage analysis on CSV, then UPDATE client_availabilities
    (start_time, end_time, minDuration) for matching records.
    """
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║   CLIENT WINDOWS ANALYZER                                      ║
    ║   Updates start_time, end_time, minDuration from visit CSV     ║
    ╚════════════════════════════════════════════════════════════════╝
    """)

    if not csv_path or not Path(csv_path).exists():
        logger.error("CSV path is required and must exist: %s", csv_path)
        return False

    connection = None
    try:
        # Stage 1–3
        df1 = stage1_load_and_clean(csv_path)
        if df1.empty:
            logger.warning("No data after Stage 1; nothing to update.")
            return True

        stage2 = stage2_initial_pattern_intelligence(df1)
        stage3 = stage3_context_aware_suggestion(stage2)
        patterns_count = len(stage3)
        logger.info("  Patterns from analysis (candidates for update): %d", patterns_count)
        if patterns_count > 0 and patterns_count <= 20:
            for _, r in stage3.iterrows():
                logger.info("    → %s | requested %s–%s → suggested %s–%s minDuration=%s",
                    r.get("Formatted_Name"), r.get("requested_start_str"), r.get("requested_end_str"),
                    r.get("start_time_str"), r.get("end_time_str"), r.get("minDuration"))

        config = get_db_config()
        connection = connect_to_database(config)
        clients_map = get_all_clients(connection)
        avail_list = load_client_availabilities(connection)
        avail_lookup = build_availability_lookup(avail_list)
        logger.info("  Matching: %d patterns vs %d clients, %d client_availabilities records",
            patterns_count, len(clients_map), len(avail_list))

        # Normalize requested times to 10-min slot for matching (same as clientAvailabilityMigration)
        def requested_key(row):
            h_s, m_s = normalize_time_for_slot(int(row["req_start_hour"]), int(row["req_start_minute"]))
            h_e, m_e = normalize_time_for_slot(int(row["req_end_hour"]), int(row["req_end_minute"]))
            req_start = min_to_time_str(h_s * 60 + m_s)
            req_end = min_to_time_str(h_e * 60 + m_e)
            return req_start, req_end

        updated = 0
        skipped_no_client = 0
        skipped_no_match = 0
        unmatched_clients = set()
        update_args = []

        for _, row in stage3.iterrows():
            # Service Location Name is "Lastname, Firstname" (e.g. "Hawkshaw (DS), Harry")
            raw = (row["Formatted_Name"] or "").strip()
            name_key = re.sub(r"\s+", " ", raw).lower() if raw else ""
            if not name_key:
                continue
            client_id = clients_map.get(name_key)
            if client_id is None:
                skipped_no_client += 1
                unmatched_clients.add(row["Formatted_Name"])
                logger.warning("No client found for name: %s", row["Formatted_Name"])
                continue

            req_start, req_end = requested_key(row)
            key = (client_id, req_start, req_end)
            recs = avail_lookup.get(key)
            if not recs:
                skipped_no_match += 1
                logger.warning(
                    "No client_availabilities record for client_id=%s requested %s–%s",
                    client_id, req_start, req_end,
                )
                continue

            start_time_str = row["start_time_str"]
            end_time_str = row["end_time_str"]
            min_dur = row.get("minDuration")
            if pd.isna(min_dur):
                min_dur = None
            else:
                min_dur = int(min_dur)

            for rec in recs:
                update_args.append((start_time_str, end_time_str, min_dur, rec["id"]))
                updated += 1

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

        # Summary report
        logger.info("")
        logger.info("=" * 60)
        logger.info("CLIENT WINDOWS ANALYZER SUMMARY")
        logger.info("=" * 60)
        logger.info("  Patterns from CSV analysis: %d", patterns_count)
        logger.info("  Updated (matched client + availability): %d", updated)
        logger.info("  Skipped (no client in DB for CSV name): %d", skipped_no_client)
        logger.info("  Skipped (no client_availabilities row for this client + requested time slot): %d", skipped_no_match)
        if unmatched_clients:
            logger.info("  Unmatched CSV client names (sample): %s", list(unmatched_clients)[:10])
        if patterns_count > 0 and updated < patterns_count:
            logger.info("  Note: Only rows that match an existing client_availabilities record (same client_id + requested_start_time + requested_end_time) are updated. Run Clients Availability first to seed records.")
        # Explain why other client_availabilities were not updated
        logger.info("  Why other records were not updated: This step only updates rows for which a suggested window was computed from the CSV. Most CSV rows were dropped in Stage 1 (e.g. Service Requirement Duration < %s min). Only %d pattern(s) were produced, so only those could be applied. To include more visits, set env CLIENT_WINDOWS_MIN_DURATION_MINUTES to a lower value (e.g. 15 or 10) and re-run.", MINIMUM_DURATION_MINUTES, patterns_count)
        logger.info("=" * 60)

        return True

    except MigrationError as e:
        logger.error("%s", e)
        return False
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        return False
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass


if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else None
    success = run(csv_path=csv_path)
    sys.exit(0 if success else 1)
