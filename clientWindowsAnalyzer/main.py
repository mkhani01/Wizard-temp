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
    print("Missing required package: %s" % e)
    print("Please install: pip install psycopg2-binary pandas numpy")
    sys.exit(1)

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

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
    out["minDuration"] = out["suggested_duration"]
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
        df1 = stage1_load_and_clean(csv_path)
        if df1.empty:
            logger.warning("No data after Stage 1; nothing to update.")
            return True
        stage2 = stage2_initial_pattern_intelligence(df1)
        stage3 = stage3_context_aware_suggestion(stage2)
        stage3 = stage3_5_remove_anomalies(stage3)
        stage3 = stage3_7_refine_duration(stage3, df1)
        patterns_count = len(stage3)
        logger.info("  Patterns from analysis (candidates for update): %d", patterns_count)
        if patterns_count > 0 and patterns_count <= 20:
            for _, r in stage3.iterrows():
                logger.info("    -> %s | requested %s-%s -> suggested %s-%s minDuration=%s",
                    r.get("Formatted_Name"), r.get("requested_start_str"), r.get("requested_end_str"),
                    r.get("start_time_str"), r.get("end_time_str"), r.get("minDuration"))
        config = get_db_config()
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
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
