#!/usr/bin/env python3
"""Unit tests for offline Preference Check and Validate-today matching helpers."""

from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from feasible_pairs_migration.preference_check import (
    build_pair_reason,
    run_preference_check,
    visit_duration_minutes,
)
from feasible_pairs_migration.profile_preferences import (
    LONG_DURATION_MINUTES,
    WEIGHT_THRESHOLD,
)
from tests.test_today_api_validation import (
    minutes_close,
    name_match_keys,
    visit_matches_slot,
)


def _write_visit_export(path: Path, rows: list) -> None:
    fieldnames = [
        "Actual Employee Name",
        "Service Location Name",
        "Planned Service Type Description",
        "Planned Service Requirement Type Description",
        "Service Requirement Service Type Description",
        "Service Requirement Start Date And Time",
        "Service Requirement End Date And Time",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class TestMinuteTolerance(unittest.TestCase):
    def test_minutes_close_within_one(self):
        self.assertTrue(minutes_close(1020, 1019))  # 17:00 vs 16:59
        self.assertTrue(minutes_close(495, 495))
        self.assertFalse(minutes_close(1020, 1018))

    def test_visit_matches_slot_ryan_case(self):
        row = {
            "name_keys": name_match_keys("*Ryan (C), Martin"),
            "start_minute": 16 * 60 + 30,
            "end_minute": 17 * 60,
        }
        visit = {
            "startMinute": 16 * 60 + 30,
            "endMinute": 16 * 60 + 59,
            "receiver": {"name": "Martin", "lastname": "Ryan"},
        }
        self.assertTrue(visit_matches_slot(visit, row))


class TestPreferenceCheck(unittest.TestCase):
    def test_build_pair_reason_must_and_only(self):
        must = build_pair_reason(
            "must", 0.97, 45, 62.0, "Current Primary", 3, 18, 29
        )
        self.assertIn(f">={WEIGHT_THRESHOLD}", must)
        self.assertIn(f"<{LONG_DURATION_MINUTES}", must)
        only = build_pair_reason(
            "only", 1.0, 360, 80.0, "Current Primary", 1, 40, 50
        )
        self.assertIn(f">={LONG_DURATION_MINUTES}", only)

    def test_visit_duration_minutes(self):
        start = datetime(2026, 7, 1, 8, 0)
        end = datetime(2026, 7, 1, 13, 0)
        self.assertEqual(visit_duration_minutes(start, end), 300)

    def test_run_preference_check_must_only_from_fixture(self):
        # One long client (ONLY) dominated by Jane; one short client (MUST) dominated by Jane.
        # Another carer on each client so weights normalize.
        end = datetime(2026, 7, 29, 12, 0)
        rows = []
        # Client Long: 6h visits — Jane dominates
        for i in range(10):
            day = end - timedelta(days=i)
            rows.append(
                {
                    "Actual Employee Name": "Doe, Jane",
                    "Service Location Name": "Long, Client",
                    "Planned Service Type Description": "Personal Care",
                    "Planned Service Requirement Type Description": "Personal Care",
                    "Service Requirement Service Type Description": "Personal Care",
                    "Service Requirement Start Date And Time": day.strftime("%d/%m/%Y 08:00"),
                    "Service Requirement End Date And Time": day.strftime("%d/%m/%Y 14:00"),
                }
            )
        for i in range(1):
            day = end - timedelta(days=i + 1)
            rows.append(
                {
                    "Actual Employee Name": "Smith, Bob",
                    "Service Location Name": "Long, Client",
                    "Planned Service Type Description": "Personal Care",
                    "Planned Service Requirement Type Description": "Personal Care",
                    "Service Requirement Service Type Description": "Personal Care",
                    "Service Requirement Start Date And Time": day.strftime("%d/%m/%Y 08:00"),
                    "Service Requirement End Date And Time": day.strftime("%d/%m/%Y 14:00"),
                }
            )
        # Client Short: 45m visits — Jane dominates
        for i in range(10):
            day = end - timedelta(days=i)
            rows.append(
                {
                    "Actual Employee Name": "Doe, Jane",
                    "Service Location Name": "Short, Client",
                    "Planned Service Type Description": "Personal Care",
                    "Planned Service Requirement Type Description": "Personal Care",
                    "Service Requirement Service Type Description": "Personal Care",
                    "Service Requirement Start Date And Time": day.strftime("%d/%m/%Y 09:00"),
                    "Service Requirement End Date And Time": day.strftime("%d/%m/%Y 09:45"),
                }
            )
        for i in range(1):
            day = end - timedelta(days=i + 2)
            rows.append(
                {
                    "Actual Employee Name": "Smith, Bob",
                    "Service Location Name": "Short, Client",
                    "Planned Service Type Description": "Personal Care",
                    "Planned Service Requirement Type Description": "Personal Care",
                    "Service Requirement Service Type Description": "Personal Care",
                    "Service Requirement Start Date And Time": day.strftime("%d/%m/%Y 09:00"),
                    "Service Requirement End Date And Time": day.strftime("%d/%m/%Y 09:45"),
                }
            )

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "visit_export.csv"
            _write_visit_export(csv_path, rows)
            ok, msgs = run_preference_check(csv_path, "Jane", "Doe")
            self.assertTrue(ok)
            text = "\n".join(msgs)
            self.assertIn('category=ONLY', text)
            self.assertIn('category=MUST', text)
            self.assertIn("SUMMARY must=", text)
            self.assertIn("Long, Client", text)
            self.assertIn("Short, Client", text)


class TestDeletedOrphanHelper(unittest.TestCase):
    def test_visit_matches_file_slot_tolerance(self):
        from updateTodayVisitsMigration.main import (
            visit_matches_file_slot,
            minutes_close,
        )

        self.assertTrue(minutes_close(1020, 1019))
        visit = {
            "receiver_client_id": 1,
            "start_minute": 990,
            "end_minute": 1019,
            "status": "UNALLOCATED",
        }
        slot = {"client_id": 1, "start_minute": 990, "end_minute": 1020}
        self.assertTrue(visit_matches_file_slot(visit, slot))


if __name__ == "__main__":
    unittest.main()
