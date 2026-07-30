"""
Offline Preference Check — same Must/Only logic as feasible-pairs migration, no DB/API.

Given a VisitExport CSV and a caregiver first/last name, compute pair weights over the
full file (required for per-client normalization), then classify ONLY/MUST/PREFERRED
for that caregiver and emit searchable reason lines.
"""

from __future__ import annotations

import csv
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from feasible_pairs_migration.feasible_pairs_migration import (
    ROSTER_WEEKS,
    ROSTER_WINDOW_DAYS,
    calculate_pair_statuses,
    calculate_pair_weights,
    find_roster_cutoff_date,
    get_actual_employee_name,
    identify_carer_status,
    is_valid_feasibility_row,
    parse_full_name,
    parse_visit_datetime,
    safe_strip,
)
from feasible_pairs_migration.profile_preferences import (
    LONG_DURATION_MINUTES,
    WEIGHT_THRESHOLD,
    classify_profile_category,
)
from encoding_utils import normalize_name_for_match

logger = logging.getLogger(__name__)

LogFn = Callable[[str], None]

VISIT_END_COLUMNS = (
    "Service Requirement End Date And Time",
    "Actual End Date And Time",
    "Planned Service Requirement End Date And Time",
    "Actual Service End Date And Time",
)


class PreferenceCheckError(Exception):
    pass


def _log(msgs: List[str], log_callback: Optional[LogFn], msg: str) -> None:
    msgs.append(msg)
    logger.info(msg)
    if log_callback:
        log_callback(msg)


def parse_visit_end_datetime(row: Dict[str, Any]) -> Optional[datetime]:
    """Parse visit end datetime (mirrors parse_visit_datetime column/format rules)."""
    date_formats = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%Y %I:%M:%S %p",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for column in VISIT_END_COLUMNS:
        raw_value = safe_strip(row.get(column, ""))
        if not raw_value:
            continue
        normalized = raw_value.replace("T", " ")
        for fmt in date_formats:
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue
        iso_value = raw_value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(iso_value)
            return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except ValueError:
            continue
    return None


def visit_duration_minutes(start: datetime, end: Optional[datetime]) -> int:
    if not end or end <= start:
        return 0
    return max(0, int(round((end - start).total_seconds() / 60.0)))


def _name_keys_for_person(first: str, last: str) -> Set[str]:
    keys: Set[str] = set()
    f = normalize_name_for_match(first)
    l = normalize_name_for_match(last)
    if f and l:
        keys.add(normalize_name_for_match(f"{f} {l}"))
        keys.add(normalize_name_for_match(f"{l}, {f}"))
    if f:
        keys.add(f)
    if l:
        keys.add(l)
    return {k for k in keys if k}


def build_offline_lookups_and_frequencies(
    csv_path: Path,
) -> Tuple[
    Dict[Tuple[int, int], int],
    Dict[Tuple[int, int], datetime],
    Dict[int, int],
    Optional[datetime],
    Dict[int, int],
    Dict[int, str],
    Dict[int, str],
    Dict[int, Set[str]],
    Dict[str, Any],
]:
    """
    Parse VisitExport and assign synthetic IDs (no DB).

    Returns:
      frequencies, pair_last_visit, customer_totals, dataset_end,
      client_durations, caregiver_names, client_names, caregiver_keys_by_id, stats
    """
    frequencies: Dict[Tuple[int, int], int] = defaultdict(int)
    pair_last_visit: Dict[Tuple[int, int], datetime] = {}
    customer_totals: Dict[int, int] = defaultdict(int)
    client_durations: Dict[int, int] = defaultdict(int)
    caregiver_names: Dict[int, str] = {}
    client_names: Dict[int, str] = {}
    caregiver_keys_by_id: Dict[int, Set[str]] = {}
    carer_id_by_key: Dict[Tuple[str, str], int] = {}
    client_id_by_key: Dict[Tuple[str, str], int] = {}
    next_carer_id = 1
    next_client_id = 1
    dataset_end: Optional[datetime] = None

    stats: Dict[str, Any] = {
        "total_rows": 0,
        "personal_care_rows": 0,
        "valid_rows": 0,
        "skipped_non_personal_care": 0,
        "skipped_invalid_datetime": 0,
        "skipped_outside_roster_window": 0,
        "skipped_missing_carer": 0,
        "skipped_missing_names": 0,
        "unique_caregivers": 0,
        "unique_clients": 0,
    }

    cutoff_date = find_roster_cutoff_date(str(csv_path))
    if cutoff_date is None:
        return {}, {}, {}, None, {}, {}, {}, {}, stats

    with open(csv_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            stats["total_rows"] += 1
            if not is_valid_feasibility_row(row):
                stats["skipped_non_personal_care"] += 1
                continue
            stats["personal_care_rows"] += 1

            employee_name = get_actual_employee_name(row)
            service_location_name = safe_strip(row.get("Service Location Name", ""))
            if not employee_name:
                stats["skipped_missing_carer"] += 1
                continue
            if not service_location_name:
                stats["skipped_missing_names"] += 1
                continue

            visit_start = parse_visit_datetime(row)
            if not visit_start:
                stats["skipped_invalid_datetime"] += 1
                continue
            if visit_start < cutoff_date:
                stats["skipped_outside_roster_window"] += 1
                continue

            employee_first, employee_last = parse_full_name(employee_name)
            client_first, client_last = parse_full_name(service_location_name)
            if not employee_first or not employee_last or not client_first or not client_last:
                stats["skipped_missing_names"] += 1
                continue

            carer_key = (employee_first.lower(), employee_last.lower())
            if carer_key not in carer_id_by_key:
                carer_id_by_key[carer_key] = next_carer_id
                caregiver_names[next_carer_id] = f"{employee_first} {employee_last}".strip()
                caregiver_keys_by_id[next_carer_id] = _name_keys_for_person(
                    employee_first, employee_last
                ) | {normalize_name_for_match(employee_name)}
                # also keys from raw display
                if "," in employee_name:
                    caregiver_keys_by_id[next_carer_id] |= {
                        normalize_name_for_match(employee_name)
                    }
                next_carer_id += 1
            caregiver_id = carer_id_by_key[carer_key]

            client_key = (client_first.lower(), client_last.lower())
            if client_key not in client_id_by_key:
                client_id_by_key[client_key] = next_client_id
                # Prefer VisitExport location display (often "Last, First")
                client_names[next_client_id] = service_location_name
                next_client_id += 1
            client_id = client_id_by_key[client_key]

            pair_key = (caregiver_id, client_id)
            frequencies[pair_key] += 1
            customer_totals[client_id] += 1
            if pair_key not in pair_last_visit or visit_start > pair_last_visit[pair_key]:
                pair_last_visit[pair_key] = visit_start
            if dataset_end is None or visit_start > dataset_end:
                dataset_end = visit_start

            visit_end = parse_visit_end_datetime(row)
            dur = visit_duration_minutes(visit_start, visit_end)
            if dur > client_durations[client_id]:
                client_durations[client_id] = dur

            stats["valid_rows"] += 1

    stats["unique_caregivers"] = len(carer_id_by_key)
    stats["unique_clients"] = len(client_id_by_key)
    return (
        dict(frequencies),
        pair_last_visit,
        dict(customer_totals),
        dataset_end,
        dict(client_durations),
        caregiver_names,
        client_names,
        caregiver_keys_by_id,
        stats,
    )


def resolve_caregiver_id(
    first_name: str,
    last_name: str,
    caregiver_names: Dict[int, str],
    caregiver_keys_by_id: Dict[int, Set[str]],
) -> Optional[int]:
    query_keys = _name_keys_for_person(first_name, last_name)
    query_keys.add(normalize_name_for_match(f"{first_name} {last_name}"))
    query_keys.add(normalize_name_for_match(f"{last_name}, {first_name}"))
    matches = [
        cid
        for cid, keys in caregiver_keys_by_id.items()
        if keys & query_keys
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        # fallback exact first/last lower on display names
        f = normalize_name_for_match(first_name)
        l = normalize_name_for_match(last_name)
        for cid, display in caregiver_names.items():
            pf, pl = parse_full_name(display)
            if pf and pl and normalize_name_for_match(pf) == f and normalize_name_for_match(pl) == l:
                return cid
        return None
    # Prefer exact first+last if multiple
    f = normalize_name_for_match(first_name)
    l = normalize_name_for_match(last_name)
    exact = []
    for cid in matches:
        pf, pl = parse_full_name(caregiver_names[cid])
        if pf and pl and normalize_name_for_match(pf) == f and normalize_name_for_match(pl) == l:
            exact.append(cid)
    if len(exact) == 1:
        return exact[0]
    return matches[0]


def build_pair_reason(
    category: str,
    weight: float,
    duration_min: int,
    overall_pct: float,
    status: str,
    days_since: int,
    pair_visits: int,
    client_visits: int,
) -> str:
    if category == "only":
        rule = f"weight={weight:.4f}>={WEIGHT_THRESHOLD} and duration_min={duration_min}>={LONG_DURATION_MINUTES}"
    elif category == "must":
        rule = f"weight={weight:.4f}>={WEIGHT_THRESHOLD} and duration_min={duration_min}<{LONG_DURATION_MINUTES}"
    elif category == "preferred":
        rule = f"weight={weight:.4f}<{WEIGHT_THRESHOLD} and status={status!r}"
    else:
        rule = "no profile preference rule matched"
    return (
        f"{rule}; overall_pct={overall_pct}; status={status!r}; "
        f"days_since={days_since}; pair_visits={pair_visits}; client_visits={client_visits}"
    )


def format_pair_log_line(
    client_name: str,
    category: str,
    weight: float,
    duration_min: int,
    overall_pct: float,
    status: str,
    days_since: int,
    reason: str,
) -> str:
    cat = category.upper() if category else "NONE"
    return (
        f'PAIR client="{client_name}" category={cat} weight={weight:.4f} '
        f"duration_min={duration_min} overall_pct={overall_pct} "
        f'status="{status}" days_since={days_since} reason="{reason}"'
    )


def run_preference_check(
    visit_export_path: str | Path,
    first_name: str,
    last_name: str,
    log_callback: Optional[LogFn] = None,
) -> Tuple[bool, List[str]]:
    """
    Offline Preference Check for one caregiver.
    Returns (ok, messages). ok is False when caregiver not found or file invalid.
    """
    msgs: List[str] = []
    csv_path = Path(visit_export_path)
    first = (first_name or "").strip()
    last = (last_name or "").strip()

    _log(msgs, log_callback, "=" * 70)
    _log(msgs, log_callback, "PREFERENCE CHECK (offline, same migration logic)")
    _log(msgs, log_callback, f'PREF_CHECK caregiver="{first} {last}"')
    _log(msgs, log_callback, f"VisitExport: {csv_path}")
    _log(msgs, log_callback, f"Rules: weight>={WEIGHT_THRESHOLD} → ONLY if duration>={LONG_DURATION_MINUTES} else MUST")
    _log(msgs, log_callback, f"Roster window: last {ROSTER_WEEKS} weeks ({ROSTER_WINDOW_DAYS} days)")
    _log(msgs, log_callback, "=" * 70)

    if not first or not last:
        _log(msgs, log_callback, "FAIL: first name and last name are required")
        return False, msgs
    if not csv_path.exists():
        _log(msgs, log_callback, f"FAIL: VisitExport file not found: {csv_path}")
        return False, msgs

    try:
        (
            frequencies,
            pair_last_visit,
            customer_totals,
            dataset_end,
            client_durations,
            caregiver_names,
            client_names,
            caregiver_keys_by_id,
            stats,
        ) = build_offline_lookups_and_frequencies(csv_path)
    except Exception as e:
        _log(msgs, log_callback, f"FAIL: could not parse VisitExport: {e}")
        return False, msgs

    _log(
        msgs,
        log_callback,
        f"PARSE total_rows={stats['total_rows']} personal_care={stats['personal_care_rows']} "
        f"valid={stats['valid_rows']} caregivers={stats['unique_caregivers']} "
        f"clients={stats['unique_clients']} "
        f"skipped_outside_window={stats['skipped_outside_roster_window']}",
    )

    if not frequencies or not dataset_end:
        _log(msgs, log_callback, "FAIL: no valid Personal Care visits in roster window")
        return False, msgs

    caregiver_id = resolve_caregiver_id(
        first, last, caregiver_names, caregiver_keys_by_id
    )
    if caregiver_id is None:
        _log(
            msgs,
            log_callback,
            f'FAIL: caregiver not found in VisitExport: "{first} {last}"',
        )
        sample = sorted(caregiver_names.values())[:15]
        if sample:
            _log(msgs, log_callback, f"Sample caregivers in file: {sample}")
        return False, msgs

    display_carer = caregiver_names.get(caregiver_id, f"{first} {last}")
    _log(msgs, log_callback, f'PREF_CHECK matched_caregiver="{display_carer}" id={caregiver_id}')

    weights = calculate_pair_weights(frequencies, pair_last_visit, customer_totals, dataset_end)
    statuses = calculate_pair_statuses(frequencies, pair_last_visit, customer_totals, dataset_end)

    counts = {"must": 0, "only": 0, "preferred": 0, "other": 0}
    pair_rows: List[Dict[str, Any]] = []

    for (cg_id, client_id), pair_visits in sorted(
        frequencies.items(),
        key=lambda item: (-weights.get(item[0], 0.0), client_names.get(item[0][1], "")),
    ):
        if cg_id != caregiver_id:
            continue
        weight = float(weights.get((cg_id, client_id), 0.0))
        status = statuses.get((cg_id, client_id), "")
        duration_min = int(client_durations.get(client_id, 0))
        client_visits = int(customer_totals.get(client_id, 0))
        overall_pct = round((pair_visits / client_visits) * 100, 1) if client_visits else 0.0
        last_visit = pair_last_visit.get((cg_id, client_id))
        days_since = 999
        if last_visit and dataset_end:
            days_since = max((dataset_end - last_visit).days, 0)
        # status already from calculate_pair_statuses; recompute for reason consistency
        _ = identify_carer_status(overall_pct, days_since)

        category = classify_profile_category(weight, duration_min, status)
        if category in counts:
            counts[category] += 1
        else:
            counts["other"] += 1

        reason = build_pair_reason(
            category or "",
            weight,
            duration_min,
            overall_pct,
            status,
            days_since,
            pair_visits,
            client_visits,
        )
        client_name = client_names.get(client_id, str(client_id))
        pair_rows.append(
            {
                "client_name": client_name,
                "category": category,
                "weight": weight,
                "duration_min": duration_min,
                "overall_pct": overall_pct,
                "status": status,
                "days_since": days_since,
                "reason": reason,
            }
        )

    _log(msgs, log_callback, "")
    _log(msgs, log_callback, "--- Must / Only (and Preferred) ---")
    # Must/Only first, then preferred, then other
    order = {"only": 0, "must": 1, "preferred": 2, None: 3}
    for row in sorted(
        pair_rows,
        key=lambda r: (order.get(r["category"], 9), -r["weight"], r["client_name"]),
    ):
        if row["category"] not in ("only", "must", "preferred"):
            continue
        _log(
            msgs,
            log_callback,
            format_pair_log_line(
                row["client_name"],
                row["category"],
                row["weight"],
                row["duration_min"],
                row["overall_pct"],
                row["status"],
                row["days_since"],
                row["reason"],
            ),
        )

    if counts["only"] == 0 and counts["must"] == 0:
        _log(msgs, log_callback, "No ONLY or MUST clients for this caregiver.")

    _log(
        msgs,
        log_callback,
        f"SUMMARY must={counts['must']} only={counts['only']} "
        f"preferred={counts['preferred']} other={counts['other']} "
        f"pairs={len(pair_rows)}",
    )
    _log(msgs, log_callback, "=" * 70)
    _log(msgs, log_callback, "RESULT: Preference Check complete")
    return True, msgs
