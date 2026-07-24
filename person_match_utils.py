"""
Person matching helpers: eircode/postcode normalization and Active-first selection.

Used when resolving clients/users by name when duplicates exist.
DB column is ``postcode`` on both ``client`` and ``user``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence

# Irish Eircode: routing key (letter + 2 digits) + unique identifier (4 alphanumerics).
# Allow optional space between parts; match trailing code in free-text addresses.
_EIRCODE_RE = re.compile(
    r"(?i)\b([A-Z]\d{2})\s*([A-Z0-9]{4})\b\s*$"
)
_EIRCODE_ANYWHERE_RE = re.compile(
    r"(?i)\b([A-Z]\d{2})\s*([A-Z0-9]{4})\b"
)


def normalize_eircode(value: Optional[str]) -> str:
    """
    Normalize an eircode/postcode for comparison: uppercase, strip spaces.
    \"F28 AC89\" / \"f28ac89\" -> \"F28AC89\".
    Returns \"\" if empty/None.
    """
    if value is None:
        return ""
    s = str(value).strip().upper()
    if not s:
        return ""
    return re.sub(r"\s+", "", s)


def extract_eircode_from_address(address: Optional[str]) -> str:
    """
    Extract a trailing Irish eircode from a free-text address.
    e.g. \"Tavanagh, Aghagower, Wesport, Mayo, F28 AC89\" -> \"F28AC89\".
    Falls back to the last eircode-like token anywhere in the string.
    """
    if address is None:
        return ""
    s = str(address).strip()
    if not s:
        return ""
    m = _EIRCODE_RE.search(s)
    if not m:
        matches = list(_EIRCODE_ANYWHERE_RE.finditer(s))
        if not matches:
            return ""
        m = matches[-1]
    return normalize_eircode(m.group(1) + m.group(2))


def _is_active(person: Dict[str, Any]) -> bool:
    return (person.get("status") or "").strip() == "Active"


def _postcode_matches(person: Dict[str, Any], source_eircode: str) -> bool:
    if not source_eircode:
        return False
    return normalize_eircode(person.get("postcode")) == source_eircode


def pick_best_person(
    candidates: Sequence[Dict[str, Any]],
    source_eircode: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Choose the best person among name-matched candidates.

    Ranking:
      1. Active + postcode matches source eircode
      2. Active (any postcode)
      3. postcode match (any status)
      4. lowest id among remaining

    ``source_eircode`` should already be normalized (or will be normalized here).
    Each candidate dict needs at least: id, status, postcode.
    """
    if not candidates:
        return None

    eircode = normalize_eircode(source_eircode) if source_eircode else ""
    people = list(candidates)

    active_postcode = [
        p for p in people if _is_active(p) and _postcode_matches(p, eircode)
    ]
    if active_postcode:
        return min(active_postcode, key=lambda p: p["id"])

    active = [p for p in people if _is_active(p)]
    if active:
        return min(active, key=lambda p: p["id"])

    postcode_match = [p for p in people if _postcode_matches(p, eircode)]
    if postcode_match:
        return min(postcode_match, key=lambda p: p["id"])

    return min(people, key=lambda p: p["id"])


def resolve_person_id(
    candidates: Sequence[Dict[str, Any]],
    source_eircode: Optional[str] = None,
) -> Optional[int]:
    """Return the chosen person's id, or None if no candidates."""
    best = pick_best_person(candidates, source_eircode)
    return None if best is None else best["id"]


def match_reason(
    chosen: Dict[str, Any],
    candidates: Sequence[Dict[str, Any]],
    source_eircode: Optional[str] = None,
) -> str:
    """Human-readable reason for logging when duplicates exist."""
    if len(candidates) <= 1:
        return "unique"
    eircode = normalize_eircode(source_eircode) if source_eircode else ""
    if _is_active(chosen) and _postcode_matches(chosen, eircode):
        return "active+postcode"
    if _is_active(chosen):
        return "active"
    if _postcode_matches(chosen, eircode):
        return "postcode"
    return "first"


def add_person_to_name_map(
    name_map: Dict[str, List[Dict[str, Any]]],
    key: str,
    person: Dict[str, Any],
) -> None:
    """Append person under a normalized name key (dedupe by id per key)."""
    if not key:
        return
    bucket = name_map.setdefault(key, [])
    pid = person["id"]
    if any(p["id"] == pid for p in bucket):
        return
    bucket.append(person)


def flatten_candidate_ids(name_map: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    """
    Convenience: resolve each name key to a single id with Active-first
    (no source eircode). Prefer using resolve_person_id per row when eircode
    is available.
    """
    out: Dict[str, int] = {}
    for key, cands in name_map.items():
        if not cands:
            continue
        pid = resolve_person_id(cands)
        if pid is not None:
            out[key] = pid
    return out
