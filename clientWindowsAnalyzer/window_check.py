"""
Offline Window Check — same Client Windows Analyzer pipeline, no DB/API.

Given a VisitExport CSV and a client first/last name, run Stages 1–3.7 filtered
to that client and emit searchable WINDOW / REASON / SUMMARY lines explaining
what schedule window would be suggested and why.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable, List, Optional, Set, Tuple

import pandas as pd

from encoding_utils import normalize_name_for_match
from feasible_pairs_migration.feasible_pairs_migration import parse_full_name
from clientWindowsAnalyzer.main import (
    DURATION_SIGNIFICANCE_THRESHOLD,
    LOWER_PERCENTILE,
    MIN_WINDOW_WIDTH_MINS,
    TOLERANCE_MINS,
    UPPER_PERCENTILE,
    compute_min_duration_from_suggested,
    stage1_load_and_clean,
    stage2_initial_pattern_intelligence,
    stage3_5_remove_anomalies,
    stage3_7_refine_duration,
    stage3_context_aware_suggestion,
)

logger = logging.getLogger(__name__)

LogFn = Callable[[str], None]


def _log(msgs: List[str], log_callback: Optional[LogFn], line: str) -> None:
    msgs.append(line)
    if log_callback:
        log_callback(line)
    logger.info(line)


def _name_keys_for_person(first: str, last: str) -> Set[str]:
    """Full-name keys only (First Last / Last, First). Never first- or last-only."""
    keys: Set[str] = set()
    f = normalize_name_for_match(first)
    l = normalize_name_for_match(last)
    if f and l:
        keys.add(normalize_name_for_match(f"{f} {l}"))
        keys.add(normalize_name_for_match(f"{l}, {f}"))
    return {k for k in keys if k}


def _formatted_name_keys(formatted_name: str) -> Set[str]:
    keys: Set[str] = set()
    raw = (formatted_name or "").strip()
    if not raw:
        return keys
    keys.add(normalize_name_for_match(raw))
    first, last = parse_full_name(raw)
    if first and last:
        keys |= _name_keys_for_person(first, last)
    return {k for k in keys if k}


def resolve_client_formatted_name(
    first_name: str,
    last_name: str,
    candidate_names: List[str],
) -> Optional[str]:
    """
    Match client first/last to a Formatted_Name value from VisitExport.

    Requires both first and last name to match (no first-only / last-only hits).
    Returns the canonical Formatted_Name string, or None if no match — callers
    must not suggest or update windows when this returns None.
    """
    f = normalize_name_for_match(first_name)
    l = normalize_name_for_match(last_name)
    if not f or not l:
        return None

    query = _name_keys_for_person(f, l)
    matches: List[str] = []
    for name in candidate_names:
        raw = (name or "").strip()
        if not raw:
            continue
        # Prefer parsed first+last equality so shared given names cannot collide
        pf, pl = parse_full_name(raw)
        if (
            pf
            and pl
            and normalize_name_for_match(pf) == f
            and normalize_name_for_match(pl) == l
        ):
            matches.append(name)
            continue
        if _formatted_name_keys(raw) & query:
            matches.append(name)

    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    return matches[0]


def build_window_reason(
    visit_count: int,
    requested_duration: int,
    suggested_duration: int,
    min_duration: int,
    window_start: str,
    window_end: str,
) -> str:
    """Human-readable rule summary matching Client Windows Analyzer stages."""
    parts = [
        f"visits={visit_count}",
        (
            f"window from actual times "
            f"(P{int(LOWER_PERCENTILE * 100)}/P{int(UPPER_PERCENTILE * 100)} "
            f"±{TOLERANCE_MINS}min, min_width={MIN_WINDOW_WIDTH_MINS}min), "
            f"clamped to requested ±{TOLERANCE_MINS}min"
        ),
        f"suggested_window={window_start}-{window_end}",
    ]
    if suggested_duration != requested_duration:
        parts.append(
            f"suggested_duration={suggested_duration} "
            f"(highest actual duration ≤ requested={requested_duration} "
            f"appearing in ≥{int(DURATION_SIGNIFICANCE_THRESHOLD * 100)}% of visits)"
        )
    else:
        parts.append(
            f"suggested_duration={suggested_duration} "
            f"(kept requested={requested_duration}; no significant shorter actual duration)"
        )
    pct = 65 if requested_duration != suggested_duration else 85
    parts.append(f"min_duration={min_duration} ({pct}% of suggested, capped by slot/requested)")
    return "; ".join(parts)


def format_window_log_line(
    client_name: str,
    requested_start: str,
    requested_end: str,
    window_start: str,
    window_end: str,
    suggested_duration: int,
    min_duration: int,
    visit_count: int,
) -> str:
    return (
        f'WINDOW client="{client_name}" '
        f'requested="{requested_start}-{requested_end}" '
        f'window="{window_start}-{window_end}" '
        f"suggested_duration={suggested_duration} min_duration={min_duration} "
        f"visits={visit_count}"
    )


def run_window_check(
    visit_export_path: str | Path,
    first_name: str,
    last_name: str,
    log_callback: Optional[LogFn] = None,
) -> Tuple[bool, List[str]]:
    """
    Offline Window Check for one client.
    Returns (ok, messages). ok is False when client not found or file invalid.
    """
    msgs: List[str] = []
    csv_path = Path(visit_export_path)
    first = (first_name or "").strip()
    last = (last_name or "").strip()

    _log(msgs, log_callback, "=" * 70)
    _log(msgs, log_callback, "WINDOW CHECK (offline, same Client Windows Analyzer logic)")
    _log(msgs, log_callback, f'WINDOW_CHECK client="{first} {last}"')
    _log(msgs, log_callback, f"VisitExport: {csv_path}")
    _log(
        msgs,
        log_callback,
        f"Rules: P{int(LOWER_PERCENTILE * 100)}/P{int(UPPER_PERCENTILE * 100)} "
        f"actual times ±{TOLERANCE_MINS}min, min width {MIN_WINDOW_WIDTH_MINS}min, "
        f"duration significance ≥{int(DURATION_SIGNIFICANCE_THRESHOLD * 100)}%",
    )
    _log(msgs, log_callback, "=" * 70)

    if not first or not last:
        _log(msgs, log_callback, "FAIL: first name and last name are required")
        return False, msgs
    if not csv_path.exists():
        _log(msgs, log_callback, f"FAIL: VisitExport file not found: {csv_path}")
        return False, msgs

    try:
        from clientWindowsAnalyzer.main import _require_runtime_deps

        _require_runtime_deps()
    except Exception as e:
        _log(msgs, log_callback, f"FAIL: missing runtime dependencies: {e}")
        return False, msgs

    try:
        stage1 = stage1_load_and_clean(str(csv_path))
    except Exception as e:
        _log(msgs, log_callback, f"FAIL: could not parse VisitExport: {e}")
        return False, msgs

    if stage1 is None or stage1.empty:
        _log(msgs, log_callback, "FAIL: no valid Personal Care visits after Stage 1 clean")
        return False, msgs

    candidates = sorted({str(n) for n in stage1["Formatted_Name"].dropna().unique()})
    _log(
        msgs,
        log_callback,
        f"PARSE stage1_rows={len(stage1)} unique_clients={len(candidates)}",
    )

    matched = resolve_client_formatted_name(first, last, candidates)
    if matched is None:
        _log(
            msgs,
            log_callback,
            f'FAIL: client not found in VisitExport: "{first} {last}"',
        )
        sample = candidates[:15]
        if sample:
            _log(msgs, log_callback, f"Sample clients in file: {sample}")
        return False, msgs

    _log(msgs, log_callback, f'WINDOW_CHECK matched_client="{matched}"')

    client_df = stage1[stage1["Formatted_Name"] == matched].copy()
    if client_df.empty:
        _log(msgs, log_callback, f'FAIL: no Stage 1 visits for matched client "{matched}"')
        return False, msgs

    _log(msgs, log_callback, f"CLIENT_VISITS rows={len(client_df)}")

    try:
        stage2 = stage2_initial_pattern_intelligence(client_df)
        if stage2.empty:
            _log(msgs, log_callback, "FAIL: no slot patterns after Stage 2")
            return False, msgs
        stage3 = stage3_context_aware_suggestion(stage2)
        if stage3.empty:
            _log(msgs, log_callback, "FAIL: no slot patterns after Stage 3")
            return False, msgs
        stage3_clean = stage3_5_remove_anomalies(stage3)
        patterns = stage3_7_refine_duration(stage3_clean, client_df)
    except Exception as e:
        _log(msgs, log_callback, f"FAIL: analysis pipeline error: {e}")
        return False, msgs

    if patterns is None or patterns.empty:
        _log(msgs, log_callback, "FAIL: no window patterns after analysis pipeline")
        return False, msgs

    _log(msgs, log_callback, "")
    _log(msgs, log_callback, "--- Suggested windows ---")

    pattern_count = 0
    for _, row in patterns.sort_values(
        by=["req_start_hour", "req_start_minute", "req_end_hour", "req_end_minute"]
    ).iterrows():
        req_start = str(row.get("requested_start_str") or "")
        req_end = str(row.get("requested_end_str") or "")
        window_start = str(row.get("start_time_str") or "")
        window_end = str(row.get("end_time_str") or "")
        suggested_val = row.get("suggested_duration")
        req_dur_val = row.get("Service Requirement Duration")
        if suggested_val is None or pd.isna(suggested_val):
            continue

        suggested_duration = int(suggested_val)
        if req_dur_val is None or pd.isna(req_dur_val):
            requested_duration = suggested_duration
        else:
            requested_duration = int(req_dur_val)

        req_start_h = int(row["req_start_hour"])
        req_start_m = int(row["req_start_minute"])
        req_end_h = int(row["req_end_hour"])
        req_end_m = int(row["req_end_minute"])
        slot_width = (req_end_h * 60 + req_end_m) - (req_start_h * 60 + req_start_m)
        suggested_duration = min(suggested_duration, requested_duration, max(slot_width, 1))
        min_duration = compute_min_duration_from_suggested(
            requested_duration, suggested_duration, slot_width
        )

        # Visit count for this required clock slot
        visit_mask = (
            (client_df["Service Requirement Start Date And Time"].dt.hour == req_start_h)
            & (client_df["Service Requirement Start Date And Time"].dt.minute == req_start_m)
            & (client_df["Service Requirement End Date And Time"].dt.hour == req_end_h)
            & (client_df["Service Requirement End Date And Time"].dt.minute == req_end_m)
        )
        visit_count = int(visit_mask.sum())

        pattern_count += 1
        _log(
            msgs,
            log_callback,
            format_window_log_line(
                matched,
                req_start,
                req_end,
                window_start,
                window_end,
                suggested_duration,
                min_duration,
                visit_count,
            ),
        )
        reason = build_window_reason(
            visit_count,
            requested_duration,
            suggested_duration,
            min_duration,
            window_start,
            window_end,
        )
        _log(msgs, log_callback, f'REASON client="{matched}" requested="{req_start}-{req_end}" {reason}')

    if pattern_count == 0:
        _log(msgs, log_callback, "No suggested windows for this client after pipeline.")

    _log(
        msgs,
        log_callback,
        f"SUMMARY client=\"{matched}\" windows={pattern_count} visits={len(client_df)}",
    )
    _log(msgs, log_callback, "=" * 70)
    _log(msgs, log_callback, "RESULT: Window Check complete")
    return True, msgs
