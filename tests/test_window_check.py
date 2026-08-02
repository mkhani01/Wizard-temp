#!/usr/bin/env python3
"""Unit tests for offline Window Check."""

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

from clientWindowsAnalyzer.window_check import (
    build_window_reason,
    resolve_client_formatted_name,
    run_window_check,
)


def _write_visit_export(path: Path, rows: list) -> None:
    fieldnames = [
        "Service Location Name",
        "Service Requirement Service Type Description",
        "Service Requirement Start Date And Time",
        "Service Requirement End Date And Time",
        "Actual Start Date And Time",
        "Actual End Date And Time",
        "Service Requirement Duration",
        "Actual Duration",
    ]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _fixture_rows_for_alice(end: datetime, count: int = 8) -> list:
    rows = []
    for i in range(count):
        day = end - timedelta(days=i)
        # Skip weekends so Stage 3 has weekday coverage
        while day.weekday() >= 5:
            day = day - timedelta(days=1)
        rows.append(
            {
                "Service Location Name": "Smith, Alice",
                "Service Requirement Service Type Description": "Personal Care",
                "Service Requirement Start Date And Time": day.strftime("%d/%m/%Y 08:00"),
                "Service Requirement End Date And Time": day.strftime("%d/%m/%Y 09:00"),
                "Actual Start Date And Time": day.strftime("%d/%m/%Y 08:05"),
                "Actual End Date And Time": day.strftime("%d/%m/%Y 08:50"),
                "Service Requirement Duration": "1.0",  # hours → 60 min
                "Actual Duration": "0.75",  # hours → 45 min
            }
        )
    return rows


class TestWindowCheckHelpers(unittest.TestCase):
    def test_resolve_client_formatted_name(self):
        candidates = ["Smith, Alice", "Jones, Bob", "Hawkshaw (DS), Harry"]
        self.assertEqual(
            resolve_client_formatted_name("Alice", "Smith", candidates),
            "Smith, Alice",
        )
        self.assertIsNone(resolve_client_formatted_name("Nobody", "Here", candidates))

    def test_resolve_requires_first_and_last_not_first_only(self):
        """Shared first name must not match a different last name (e.g. Fiona)."""
        candidates = ["Buchannon (DS), Fiona", "Smith, Alice"]
        self.assertIsNone(
            resolve_client_formatted_name("Fiona", "McKinnon", candidates)
        )
        self.assertEqual(
            resolve_client_formatted_name("Fiona", "Buchannon (DS)", candidates),
            "Buchannon (DS), Fiona",
        )

    def test_build_window_reason_includes_rules(self):
        reason = build_window_reason(8, 60, 45, 29, "08:00:00", "09:00:00")
        self.assertIn("visits=8", reason)
        self.assertIn("suggested_duration=45", reason)
        self.assertIn("min_duration=29", reason)
        self.assertIn("65%", reason)


class TestWindowCheckRun(unittest.TestCase):
    def test_known_client_emits_window_and_reason(self):
        end = datetime(2026, 7, 29, 12, 0)
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "visit_export.csv"
            _write_visit_export(csv_path, _fixture_rows_for_alice(end))
            ok, msgs = run_window_check(csv_path, "Alice", "Smith")
            self.assertTrue(ok)
            joined = "\n".join(msgs)
            self.assertIn("WINDOW ", joined)
            self.assertIn("REASON ", joined)
            self.assertIn("SUMMARY ", joined)
            self.assertIn('client="Smith, Alice"', joined)
            self.assertIn("RESULT: Window Check complete", joined)

    def test_unknown_client_fails(self):
        end = datetime(2026, 7, 29, 12, 0)
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "visit_export.csv"
            _write_visit_export(csv_path, _fixture_rows_for_alice(end))
            ok, msgs = run_window_check(csv_path, "Nobody", "Here")
            self.assertFalse(ok)
            joined = "\n".join(msgs)
            self.assertIn("FAIL: client not found", joined)


if __name__ == "__main__":
    unittest.main()
