"""
CSV parser for Availability Types migration.
Maps CSV columns (Name, Type, Description, Is Paid, Color, Category) to the
NestJS AvailabilityType entity: name, type, category, description, color, icon, is_paid.
"""

import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Entity enum values (must match NestJS AvailabilityTypeEnum and AvailabilityCategory)
AVAILABILITY_TYPE_VALUES = {"availability", "unavailability"}
AVAILABILITY_CATEGORY_VALUES = {"CLIENT", "USER", "BOTH"}

# CSV column name variants (user CSV uses "Name", "Type", "Is Paid", "Category")
CSV_COLUMNS = {
    "name": ["name", "Name"],
    "type": ["type", "Type"],
    "description": ["description", "Description"],
    "is_paid": ["is_paid", "Is Paid", "is paid", "IsPaid"],
    "color": ["color", "Color"],
    "icon": ["icon", "Icon"],
    "category": ["category", "Category"],
}


def _safe_strip(value):
    if value is None:
        return ""
    return str(value).strip()


def _get_column(row, keys):
    """Return first matching column value from row (case-sensitive key match)."""
    for key in keys:
        if key in row:
            return row[key]
    return None


def _parse_type(value):
    """Normalize Type column to entity enum: availability | unavailability."""
    raw = _safe_strip(value)
    if not raw:
        return None
    lower = raw.lower().strip()
    if lower in ("availability", "availability "):
        return "availability"
    if lower in ("unavailability", "unavailability ", "unavailability  "):
        return "unavailability"
    # Allow flexible matching
    if "avail" in lower and "un" not in lower:
        return "availability"
    if "unavail" in lower or "un availability" in lower:
        return "unavailability"
    return None


def _parse_category(value):
    """Normalize Category column to entity enum: CLIENT | USER | BOTH."""
    raw = _safe_strip(value)
    if not raw:
        return None
    lower = raw.lower()
    if lower in ("user", "users"):
        return "USER"
    if lower in ("client", "clients"):
        return "CLIENT"
    if lower in ("both", "all"):
        return "BOTH"
    # Try exact match
    if raw.upper() in AVAILABILITY_CATEGORY_VALUES:
        return raw.upper()
    return None


def _parse_is_paid(value):
    """Parse Is Paid column to boolean (YES/NO, true/false, 1/0)."""
    raw = _safe_strip(value)
    if not raw:
        return False
    lower = raw.lower()
    if lower in ("yes", "true", "1", "y"):
        return True
    if lower in ("no", "false", "0", "n"):
        return False
    return False


def extract_from_csv(csv_path):
    """
    Extract availability type rows from a CSV file.

    Supports CSV format with columns:
      Name, Type, Description, Is Paid, Color, Category

    Returns list of dicts with keys: name, type, category, description, color, icon, is_paid.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    types = []
    logger.info("Reading CSV: %s", csv_path)

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        row_num = 0

        for row in reader:
            row_num += 1

            name = _safe_strip(_get_column(row, CSV_COLUMNS["name"]))
            if not name:
                logger.warning(
                    "Row %d: SKIPPED - missing name | row=%s",
                    row_num, {k: v for k, v in row.items() if v}
                )
                continue

            type_value = _parse_type(_get_column(row, CSV_COLUMNS["type"]))
            if not type_value:
                logger.warning(
                    "Row %d: SKIPPED - invalid type | name=%r, type_raw=%r, category=%r",
                    row_num, name, _get_column(row, CSV_COLUMNS["type"]), _get_column(row, CSV_COLUMNS["category"])
                )
                continue

            category_value = _parse_category(_get_column(row, CSV_COLUMNS["category"]))
            if not category_value:
                # Default to USER when all rows are "User" in your CSV
                category_value = "USER"
                logger.debug("Row %s: Category missing or unknown, defaulting to USER", row_num)

            description = _safe_strip(_get_column(row, CSV_COLUMNS["description"])) or None
            color = _safe_strip(_get_column(row, CSV_COLUMNS["color"])) or None
            icon = _safe_strip(_get_column(row, CSV_COLUMNS["icon"])) or None
            is_paid = _parse_is_paid(_get_column(row, CSV_COLUMNS["is_paid"]))

            types.append({
                "name": name,
                "type": type_value,
                "category": category_value,
                "description": description,
                "color": color,
                "icon": icon,
                "is_paid": is_paid,
            })
            logger.info(
                "Row %d: ADDED | name=%r, type=%s, category=%s, is_paid=%s",
                row_num, name, type_value, category_value, is_paid
            )

    logger.info("Extracted %s availability types from CSV", len(types))
    return types


def deduplicate(types):
    """Deduplicate by (name, type, category). Name comparison is case-insensitive. Keeps last occurrence."""
    unique_map = {}
    for t in types:
        key = ((t.get("name") or "").strip().lower(), t["type"], t["category"])
        unique_map[key] = t
    result = list(unique_map.values())
    if len(result) != len(types):
        logger.info(
            "Deduplicated: %s → %s unique records",
            len(types), len(result),
        )
    return result
