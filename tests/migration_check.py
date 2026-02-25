#!/usr/bin/env python3
"""
Post-migration validation checks.

After all migrations complete, the user can click "Check the migration" to run
these tests.  Each migration type has its own check function that compares the
input files against the database to verify correctness.

Every check function returns (passed: bool, messages: list[str]).
"""

import csv
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None

try:
    import openpyxl
except ImportError:
    openpyxl = None

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_assets_dir() -> Path:
    from migration_support import get_assets_dir
    return get_assets_dir()


def _safe_strip(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


# ---------------------------------------------------------------------------
# 1. Caregivers (Users) Migration Check
# ---------------------------------------------------------------------------

def check_caregivers(connection) -> Tuple[bool, List[str]]:
    """
    Verify that every valid record in the caregivers CSV exists in the
    ``"user"`` table with ``status = 'Active'``.
    """
    msgs: List[str] = []
    csv_path = _get_assets_dir() / "CareAssistantExport.csv"

    if not csv_path.exists():
        msgs.append("SKIP: Caregivers CSV not found at %s" % csv_path)
        return True, msgs

    # Read expected names from CSV
    expected = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            first = _safe_strip(row.get("First Name"))
            last = _safe_strip(row.get("Last Name"))
            if not last and first and " " in first:
                parts = first.split()
                titles = {"Mr", "Mrs", "Miss", "Ms", "Dr", "Prof",
                          "Mr.", "Mrs.", "Miss.", "Ms.", "Dr.", "Prof."}
                while parts and parts[0] in titles:
                    parts = parts[1:]
                if len(parts) >= 2:
                    first = parts[0]
                    last = " ".join(parts[1:])
            if first and last:
                expected.append((first.lower(), last.lower()))

    if not expected:
        msgs.append("WARN: No valid records found in CSV")
        return True, msgs

    # Query DB
    cursor = connection.cursor()
    try:
        cursor.execute(
            'SELECT name, lastname, status FROM "user" WHERE deleted_at IS NULL'
        )
        db_rows = cursor.fetchall()
    finally:
        cursor.close()

    db_map: Dict[Tuple[str, str], str] = {}
    for row in db_rows:
        key = ((row["name"] or "").strip().lower(),
               (row["lastname"] or "").strip().lower())
        db_map[key] = (row["status"] or "").strip()

    missing = []
    wrong_status = []
    for first, last in expected:
        key = (first, last)
        if key not in db_map:
            missing.append("%s %s" % (first.title(), last.title()))
        elif db_map[key] != "Active":
            wrong_status.append(
                "%s %s (status=%s)" % (first.title(), last.title(), db_map[key])
            )

    msgs.append("Caregivers: %d expected from CSV, %d in DB" % (len(expected), len(db_map)))

    if missing:
        msgs.append("FAIL: %d caregiver(s) missing from DB:" % len(missing))
        for m in missing[:20]:
            msgs.append("  - %s" % m)
        if len(missing) > 20:
            msgs.append("  ... and %d more" % (len(missing) - 20))

    if wrong_status:
        msgs.append("FAIL: %d caregiver(s) not Active:" % len(wrong_status))
        for w in wrong_status[:20]:
            msgs.append("  - %s" % w)
        if len(wrong_status) > 20:
            msgs.append("  ... and %d more" % (len(wrong_status) - 20))

    passed = not missing and not wrong_status
    if passed:
        msgs.append("PASS: All %d caregivers present with Active status" % len(expected))
    return passed, msgs


# ---------------------------------------------------------------------------
# 2. Clients Migration Check
# ---------------------------------------------------------------------------

def check_clients(connection) -> Tuple[bool, List[str]]:
    """
    - Every client in the CSV must exist in ``client`` with Active status.
    - Every client NOT in the CSV must have ``status = 'Deactive'``.
    """
    msgs: List[str] = []
    csv_path = _get_assets_dir() / "CustomerExport.csv"

    if not csv_path.exists():
        msgs.append("SKIP: Clients CSV not found at %s" % csv_path)
        return True, msgs

    # Read expected clients
    expected_keys = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        for row in reader:
            first = _safe_strip(row.get("First Name"))
            last = _safe_strip(row.get("Last Name"))
            if first and last:
                expected_keys.add((first.lower(), last.lower()))

    if not expected_keys:
        msgs.append("WARN: No valid client records found in CSV")
        return True, msgs

    # Query DB
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT name, lastname, status FROM client WHERE deleted_at IS NULL"
        )
        db_rows = cursor.fetchall()
    finally:
        cursor.close()

    missing = []
    wrong_status_active = []   # should be Active but isn't
    wrong_status_deactive = [] # should be Deactive but isn't

    for row in db_rows:
        key = ((row["name"] or "").strip().lower(),
               (row["lastname"] or "").strip().lower())
        status = (row["status"] or "").strip()
        if key in expected_keys:
            if status != "Active":
                wrong_status_active.append(
                    "%s %s (status=%s)" % (key[0].title(), key[1].title(), status)
                )
        else:
            if status != "Deactive":
                wrong_status_deactive.append(
                    "%s %s (status=%s)" % (key[0].title(), key[1].title(), status)
                )

    # Check if any expected client is missing entirely
    db_keys = set()
    for row in db_rows:
        db_keys.add(((row["name"] or "").strip().lower(),
                      (row["lastname"] or "").strip().lower()))
    for key in expected_keys:
        if key not in db_keys:
            missing.append("%s %s" % (key[0].title(), key[1].title()))

    msgs.append("Clients: %d expected from CSV, %d in DB" % (len(expected_keys), len(db_rows)))

    if missing:
        msgs.append("FAIL: %d client(s) missing from DB:" % len(missing))
        for m in missing[:20]:
            msgs.append("  - %s" % m)
        if len(missing) > 20:
            msgs.append("  ... and %d more" % (len(missing) - 20))

    if wrong_status_active:
        msgs.append("FAIL: %d client(s) from CSV not Active:" % len(wrong_status_active))
        for w in wrong_status_active[:20]:
            msgs.append("  - %s" % w)
        if len(wrong_status_active) > 20:
            msgs.append("  ... and %d more" % (len(wrong_status_active) - 20))

    if wrong_status_deactive:
        msgs.append("FAIL: %d client(s) NOT in CSV should be Deactive:" % len(wrong_status_deactive))
        for w in wrong_status_deactive[:20]:
            msgs.append("  - %s" % w)
        if len(wrong_status_deactive) > 20:
            msgs.append("  ... and %d more" % (len(wrong_status_deactive) - 20))

    passed = not missing and not wrong_status_active and not wrong_status_deactive
    if passed:
        msgs.append("PASS: All clients validated (CSV=Active, others=Deactive)")
    return passed, msgs


# ---------------------------------------------------------------------------
# 3. Client Availability Migration Check
# ---------------------------------------------------------------------------

def check_client_availability(connection) -> Tuple[bool, List[str]]:
    """
    - Filter the Excel input: keep only rows where both
      ``Planned Service Type Description`` and
      ``Planned Service Requirement Type Description`` equal "Personal Care".
    - Each filtered date must have a corresponding record in
      ``client_availabilities``.
    """
    msgs: List[str] = []
    xlsx_path = _get_assets_dir() / "clientAvailability" / "ClientHoursWithServiceType.xlsx"

    if not xlsx_path.exists():
        msgs.append("SKIP: Client availability XLSX not found at %s" % xlsx_path)
        return True, msgs

    if openpyxl is None:
        msgs.append("SKIP: openpyxl not installed, cannot read XLSX")
        return True, msgs

    # Parse Excel - extract Personal Care records
    try:
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    except Exception as e:
        msgs.append("FAIL: Cannot open XLSX: %s" % e)
        return False, msgs

    sheet_name = "Data"
    if sheet_name not in wb.sheetnames:
        msgs.append("FAIL: Sheet 'Data' not found in XLSX")
        return False, msgs

    ws = wb[sheet_name]
    header = [str(c or "").strip() for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]

    def _col_idx(names):
        for n in names:
            nl = n.lower()
            for i, h in enumerate(header):
                if h.lower() == nl:
                    return i
        return -1

    col_loc = _col_idx(["Service Location Name"])
    col_type = _col_idx(["Planned Service Type Description"])
    col_req = _col_idx(["Planned Service Requirement Type Description"])
    col_start = _col_idx(["Service Requirement Start Date And Time"])

    if any(c == -1 for c in [col_loc, col_type, col_req, col_start]):
        msgs.append("FAIL: Missing required columns in XLSX")
        return False, msgs

    # Collect all client names from filtered Personal Care rows
    personal_care_clients = set()
    filtered_count = 0
    total_count = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        total_count += 1
        stype = _safe_strip(row[col_type] if col_type < len(row) else None)
        sreq = _safe_strip(row[col_req] if col_req < len(row) else None)

        if stype.lower() != "personal care" or sreq.lower() != "personal care":
            continue

        filtered_count += 1
        loc = _safe_strip(row[col_loc] if col_loc < len(row) else None)
        if loc:
            personal_care_clients.add(loc.lower())

    msgs.append("Client Availability: %d total rows, %d Personal Care rows, %d unique clients"
                 % (total_count, filtered_count, len(personal_care_clients)))

    if not personal_care_clients:
        msgs.append("WARN: No Personal Care records found in XLSX")
        return True, msgs

    # Check DB for client_availabilities
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT c.name, c.lastname
            FROM client_availabilities ca
            JOIN client c ON c.id = ca.client_id
            WHERE ca.deleted_at IS NULL AND c.deleted_at IS NULL
        """)
        db_clients = set()
        for row in cursor.fetchall():
            name = _safe_strip(row["name"])
            lastname = _safe_strip(row["lastname"])
            key = ("%s, %s" % (lastname, name)).lower()
            db_clients.add(key)
    finally:
        cursor.close()

    missing = []
    for client_key in personal_care_clients:
        if client_key not in db_clients:
            missing.append(client_key)

    if missing:
        msgs.append("FAIL: %d client(s) from XLSX have no availability records in DB:" % len(missing))
        for m in sorted(missing)[:20]:
            msgs.append("  - %s" % m)
        if len(missing) > 20:
            msgs.append("  ... and %d more" % (len(missing) - 20))

    passed = not missing
    if passed:
        msgs.append("PASS: All %d Personal Care clients have availability records" % len(personal_care_clients))
    return passed, msgs


# ---------------------------------------------------------------------------
# 4. Caregivers (User) Availability Migration Check
# ---------------------------------------------------------------------------

def check_caregiver_availability(connection) -> Tuple[bool, List[str]]:
    """
    Same logic as client availability but for user_availabilities.
    Reads the caregivers availability XLSX and verifies all users have records.
    """
    msgs: List[str] = []
    xlsx_path = _get_assets_dir() / "userAvailabilities" / "userAvailabilities.xlsx"

    if not xlsx_path.exists():
        msgs.append("SKIP: Caregiver availability XLSX not found at %s" % xlsx_path)
        return True, msgs

    if openpyxl is None:
        msgs.append("SKIP: openpyxl not installed, cannot read XLSX")
        return True, msgs

    try:
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    except Exception as e:
        msgs.append("FAIL: Cannot open XLSX: %s" % e)
        return False, msgs

    # Use first sheet (or 'Care Assistant Availability')
    sheet_name = "Care Assistant Availability"
    if sheet_name not in wb.sheetnames:
        sheet_name = wb.sheetnames[0]

    ws = wb[sheet_name]

    # Column 0 = Care Assistant Name (based on userAvailabilityMigration)
    COL_NAME = 0
    expected_users = set()
    total_count = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        total_count += 1
        name = _safe_strip(row[COL_NAME] if len(row) > COL_NAME else None)
        if name:
            # Strip title
            parts = name.split()
            titles = {"Mr", "Mrs", "Miss", "Ms", "Dr", "Prof",
                      "Mr.", "Mrs.", "Miss.", "Ms.", "Dr.", "Prof."}
            while parts and parts[0] in titles:
                parts = parts[1:]
            clean_name = " ".join(parts).lower()
            if clean_name:
                expected_users.add(clean_name)

    msgs.append("Caregiver Availability: %d total rows, %d unique users" % (total_count, len(expected_users)))

    if not expected_users:
        msgs.append("WARN: No user records found in XLSX")
        return True, msgs

    # Check DB
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT u.name, u.lastname
            FROM user_availabilities ua
            JOIN "user" u ON u.id = ua.user_id
            WHERE ua.deleted_at IS NULL AND u.deleted_at IS NULL
        """)
        db_users = set()
        for row in cursor.fetchall():
            name = _safe_strip(row["name"])
            lastname = _safe_strip(row["lastname"])
            key = ("%s %s" % (name, lastname)).lower()
            db_users.add(key)
    finally:
        cursor.close()

    missing = []
    for user_key in expected_users:
        if user_key not in db_users:
            missing.append(user_key)

    if missing:
        msgs.append("FAIL: %d user(s) from XLSX have no availability records in DB:" % len(missing))
        for m in sorted(missing)[:20]:
            msgs.append("  - %s" % m)
        if len(missing) > 20:
            msgs.append("  ... and %d more" % (len(missing) - 20))

    passed = not missing
    if passed:
        msgs.append("PASS: All %d caregivers have availability records" % len(expected_users))
    return passed, msgs


# ---------------------------------------------------------------------------
# 5. Distances Migration Check
# ---------------------------------------------------------------------------

def check_distances(connection) -> Tuple[bool, List[str]]:
    """
    Verify that all (user, client) pairs with coordinates have entries in
    ``travel_distances``.
    """
    msgs: List[str] = []

    cursor = connection.cursor()
    try:
        # Get all users with coordinates
        cursor.execute("""
            SELECT id FROM "user"
            WHERE deleted_at IS NULL AND is_caregiver = true
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        user_ids = [row["id"] for row in cursor.fetchall()]

        # Get all clients with coordinates
        cursor.execute("""
            SELECT id FROM client
            WHERE deleted_at IS NULL
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        client_ids = [row["id"] for row in cursor.fetchall()]

        if not user_ids or not client_ids:
            msgs.append("SKIP: No users (%d) or clients (%d) with coordinates"
                         % (len(user_ids), len(client_ids)))
            return True, msgs

        expected_pairs = len(user_ids) * len(client_ids)
        msgs.append("Distances: %d users x %d clients = %d expected pairs (per travel method)"
                     % (len(user_ids), len(client_ids), expected_pairs))

        # Count actual pairs per travel method
        cursor.execute("""
            SELECT travel_method, COUNT(*) as cnt
            FROM travel_distances
            WHERE from_type = 'user' AND to_type = 'client'
            GROUP BY travel_method
        """)
        method_counts = {row["travel_method"]: row["cnt"] for row in cursor.fetchall()}

    finally:
        cursor.close()

    methods = ["car", "bike", "walk"]
    all_ok = True
    for method in methods:
        actual = method_counts.get(method, 0)
        if actual < expected_pairs:
            msgs.append("FAIL: travel_method=%s has %d/%d pairs" % (method, actual, expected_pairs))
            all_ok = False
        else:
            msgs.append("OK: travel_method=%s has %d/%d pairs" % (method, actual, expected_pairs))

    if all_ok:
        msgs.append("PASS: All distance pairs present for all travel methods")
    return all_ok, msgs


# ---------------------------------------------------------------------------
# 6. Availability Types Check (placeholder)
# ---------------------------------------------------------------------------

def check_availability_types(connection) -> Tuple[bool, List[str]]:
    msgs: List[str] = []
    msgs.append("SKIP: Availability types check — no specific scenario defined yet")
    return True, msgs


# ---------------------------------------------------------------------------
# 7. Geocode Checks (placeholder)
# ---------------------------------------------------------------------------

def check_geocode(connection) -> Tuple[bool, List[str]]:
    msgs: List[str] = []
    msgs.append("SKIP: Geocode check — no specific scenario defined yet")
    return True, msgs


# ---------------------------------------------------------------------------
# 8. Feasible Pairs Check (placeholder)
# ---------------------------------------------------------------------------

def check_feasible_pairs(connection) -> Tuple[bool, List[str]]:
    msgs: List[str] = []
    msgs.append("SKIP: Feasible pairs check — no specific scenario defined yet")
    return True, msgs


# ---------------------------------------------------------------------------
# 9. Client Windows Check (placeholder)
# ---------------------------------------------------------------------------

def check_client_windows(connection) -> Tuple[bool, List[str]]:
    msgs: List[str] = []
    msgs.append("SKIP: Client windows check — no specific scenario defined yet")
    return True, msgs


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

# Maps wizard migration option keys to check functions.
CHECK_MAP = {
    "caregivers":              ("Caregivers",              check_caregivers),
    "clients":                 ("Clients",                 check_clients),
    "clients_availability":    ("Clients Availability",    check_client_availability),
    "caregivers_availability": ("Caregivers Availability", check_caregiver_availability),
    "calculate_distances":     ("Distances",               check_distances),
    "availability_types":      ("Availability Types",      check_availability_types),
    "geocode_api":             ("Geocode (API)",           check_geocode),
    "geocode_client_file":     ("Geocode Client File",     check_geocode),
    "geocode_caregiver_file":  ("Geocode Caregiver File",  check_geocode),
    "fvisit_history":          ("Feasible Pairs",          check_feasible_pairs),
    "client_windows":          ("Client Windows",          check_client_windows),
}


def run_migration_checks(
    connection,
    selected_options: List[str],
    log_callback=None,
) -> Tuple[bool, List[str]]:
    """
    Run post-migration checks for the given option keys.

    Parameters
    ----------
    connection : psycopg2 connection
    selected_options : list of option key strings that were migrated
    log_callback : optional callable(str) invoked for every message line

    Returns
    -------
    (all_passed, all_messages)
    """
    all_passed = True
    all_msgs: List[str] = []

    def _log(msg):
        all_msgs.append(msg)
        if log_callback:
            log_callback(msg)

    _log("=" * 60)
    _log("POST-MIGRATION VALIDATION CHECKS")
    _log("=" * 60)
    _log("")

    for opt_key in selected_options:
        entry = CHECK_MAP.get(opt_key)
        if entry is None:
            continue
        label, check_fn = entry
        _log("--- %s ---" % label)
        try:
            passed, msgs = check_fn(connection)
        except Exception as e:
            passed = False
            msgs = ["ERROR: %s" % e]
            logger.exception("Check %s failed", label)
        for m in msgs:
            _log("  %s" % m)
        if not passed:
            all_passed = False
        _log("")

    _log("=" * 60)
    if all_passed:
        _log("ALL CHECKS PASSED")
    else:
        _log("SOME CHECKS FAILED — review messages above")
    _log("=" * 60)

    return all_passed, all_msgs
