"""
Shared encoding and name normalization for all migrations.

Use this module everywhere we read or compare name/lastname (or "Service Location Name")
from CSV, Excel, or DB so that encoding (e.g. O‚ÄôCeallaigh vs O'Ceallaigh) never causes
mismatches or duplicates across clients, users, availability, and checks.

- fix_utf8_mojibake: repair mojibake from wrong encoding (apply to every string from CSV/Excel).
- normalize_name_for_match: canonical form for matching (lowercase, apostrophe variants -> ', collapse spaces).
- normalize_name_for_client_match: alias for normalize_name_for_match (same key for client matching; no stripping).
"""

from typing import Optional


# ---------------------------------------------------------------------------
# UTF-8 mojibake fix (e.g. O‚ÄôCeallaigh -> O'Ceallaigh)
# ---------------------------------------------------------------------------

def fix_utf8_mojibake(value: Optional[str]) -> str:
    """
    Fix common UTF-8 mojibake when CSV/Excel was saved with wrong encoding.
    e.g. "O‚ÄôCeallaigh" (wrong) -> "O'Ceallaigh" (U+2019 RIGHT SINGLE QUOTATION MARK).

    Apply to every string read from CSV/Excel before storing or comparing.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        return str(value)
    s = value
    replacements = [
        ("\u201a\u00c4\u00f4", "\u2019"),   # ‚Äô -> '
        ("â€™", "\u2019"),                   # common alternative mojibake
        ("\u201a\u00c4\u00fa", "\u201c"),  # ‚Äú -> "
        ("\u201a\u00c4\u00fb", "\u201d"),  # ‚Äù -> "
    ]
    for wrong, right in replacements:
        s = s.replace(wrong, right)
    return s


# ---------------------------------------------------------------------------
# Name normalization for matching (same key for O'Ceallaigh / O'Ceallaigh / O‚ÄôCeallaigh after fix)
# ---------------------------------------------------------------------------

def normalize_name_for_match(value: Optional[str]) -> str:
    """
    Normalize a name (or "lastname, name" / "name lastname") for comparison.
    - Strip and lowercase
    - Replace apostrophe-like chars (U+2019, U+2018, prime, acute) with ASCII '
    - Collapse whitespace to single spaces

    Use for: building lookup keys, comparing CSV row to DB row, deduplication.
    """
    if value is None or not str(value).strip():
        return ""
    s = str(value).strip().lower()
    for char in ("\u2019", "\u2018", "\u2032", "\u00b4"):  # ', ', ′, ´
        s = s.replace(char, "'")
    return " ".join(s.split())


def normalize_name_for_client_match(value: Optional[str]) -> str:
    """
    Normalize name for client matching. Same as normalize_name_for_match (exact match
    after lowercasing and apostrophe normalization). Use when building client lookup
    keys so CSV and DB keys match when the name/lastname are the same (e.g. "Hawkshaw (DS)").
    """
    return normalize_name_for_match(value)
