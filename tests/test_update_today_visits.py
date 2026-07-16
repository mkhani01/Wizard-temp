#!/usr/bin/env python3
"""Unit tests for update today visits helpers and schedule Actual fallback."""

import sys
import unittest
from datetime import datetime, date
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from updateTodayVisitsMigration.main import (
    datetime_to_minutes,
    parse_datetime_value,
    parse_target_date,
    resolve_start_end,
    match_and_cancel_from_file,
    cancel_terminated_client_visits,
    TERMINATED_CANCELLATION_TYPE,
)
from clientAvailabilityMigration.main import parse_datetime_value as schedule_parse_datetime


class TestDatetimeHelpers(unittest.TestCase):
    def test_parse_target_date_iso(self):
        self.assertEqual(parse_target_date("2026-07-16"), date(2026, 7, 16))

    def test_datetime_to_minutes(self):
        self.assertEqual(datetime_to_minutes(datetime(2026, 7, 16, 9, 30)), 570)
        self.assertEqual(datetime_to_minutes(datetime(2026, 7, 16, 0, 0)), 0)

    def test_resolve_start_end_prefers_requirement(self):
        start, end, source = resolve_start_end(
            "16-07-2026 09:00:00",
            "16-07-2026 10:00:00",
            "16-07-2026 08:00:00",
            "16-07-2026 09:00:00",
        )
        self.assertEqual(start, datetime(2026, 7, 16, 9, 0))
        self.assertEqual(end, datetime(2026, 7, 16, 10, 0))
        self.assertEqual(source, "requirement")

    def test_resolve_start_end_falls_back_to_actual(self):
        start, end, source = resolve_start_end(
            None,
            None,
            "16-07-2026 08:15:00",
            "16-07-2026 09:15:00",
        )
        self.assertEqual(start, datetime(2026, 7, 16, 8, 15))
        self.assertEqual(end, datetime(2026, 7, 16, 9, 15))
        self.assertEqual(source, "actual")

    def test_resolve_start_end_both_empty(self):
        start, end, source = resolve_start_end(None, None, None, "")
        self.assertIsNone(start)
        self.assertIsNone(end)

    def test_schedule_parse_datetime_matches(self):
        self.assertEqual(
            schedule_parse_datetime("16-07-2026 09:00:00"),
            parse_datetime_value("16-07-2026 09:00:00"),
        )


class TestMatchAndCancel(unittest.TestCase):
    def test_match_cancels_by_client_and_minutes(self):
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value = cursor
        cursor.rowcount = 1

        visits = [
            {
                "id": "v1",
                "receiver_client_id": 10,
                "start_minute": 540,
                "end_minute": 600,
                "status": "UNALLOCATED",
                "cancellation_type_id": None,
            },
            {
                "id": "v2",
                "receiver_client_id": 10,
                "start_minute": 700,
                "end_minute": 760,
                "status": "ALLOCATED",
                "cancellation_type_id": None,
            },
        ]
        cancel_rows = [
            {
                "row_num": 2,
                "client_id": 10,
                "start_minute": 540,
                "end_minute": 600,
                "cancellation_name": "Hospital",
            }
        ]
        type_ids = {"Hospital": 5}

        cancelled, skipped = match_and_cancel_from_file(
            connection, visits, cancel_rows, type_ids
        )
        self.assertEqual(cancelled, 1)
        self.assertEqual(skipped, 0)
        self.assertEqual(visits[0]["status"], "CANCELLED")
        self.assertEqual(visits[0]["cancellation_type_id"], 5)
        self.assertEqual(visits[1]["status"], "ALLOCATED")
        connection.commit.assert_called()

    def test_unmatched_row_skipped(self):
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value = cursor

        visits = [
            {
                "id": "v1",
                "receiver_client_id": 10,
                "start_minute": 540,
                "end_minute": 600,
                "status": "UNALLOCATED",
                "cancellation_type_id": None,
            }
        ]
        cancel_rows = [
            {
                "row_num": 3,
                "client_id": 99,
                "start_minute": 540,
                "end_minute": 600,
                "cancellation_name": "Hospital",
            }
        ]
        cancelled, skipped = match_and_cancel_from_file(
            connection, visits, cancel_rows, {"Hospital": 5}
        )
        self.assertEqual(cancelled, 0)
        self.assertEqual(skipped, 1)


class TestTerminatedCancel(unittest.TestCase):
    def test_cancels_only_terminated_active_visits(self):
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value = cursor
        cursor.rowcount = 1

        visits = [
            {
                "id": "v1",
                "receiver_client_id": 1,
                "start_minute": 540,
                "end_minute": 600,
                "status": "ALLOCATED",
                "cancellation_type_id": None,
            },
            {
                "id": "v2",
                "receiver_client_id": 2,
                "start_minute": 540,
                "end_minute": 600,
                "status": "UNALLOCATED",
                "cancellation_type_id": None,
            },
            {
                "id": "v3",
                "receiver_client_id": 1,
                "start_minute": 700,
                "end_minute": 760,
                "status": "CANCELLED",
                "cancellation_type_id": 9,
            },
        ]
        count = cancel_terminated_client_visits(
            connection, visits, terminated_ids={1}, terminated_type_id=42
        )
        self.assertEqual(count, 1)
        self.assertEqual(visits[0]["status"], "CANCELLED")
        self.assertEqual(visits[0]["cancellation_type_id"], 42)
        self.assertEqual(visits[1]["status"], "UNALLOCATED")
        self.assertEqual(TERMINATED_CANCELLATION_TYPE, "Terminated")


class TestScheduleActualFallback(unittest.TestCase):
    def test_requirement_empty_uses_actual_parse(self):
        req = schedule_parse_datetime(None)
        act = schedule_parse_datetime("16-07-2026 11:00:00")
        self.assertIsNone(req)
        self.assertEqual(act, datetime(2026, 7, 16, 11, 0))

    def test_both_empty_is_none(self):
        self.assertIsNone(schedule_parse_datetime(None))
        self.assertIsNone(schedule_parse_datetime(""))

    def test_process_xlsx_uses_actual_when_requirement_empty(self):
        import tempfile
        import openpyxl
        from clientAvailabilityMigration.main import process_xlsx_file
        from encoding_utils import normalize_name_for_match

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Data"
        ws.append([
            "Service Location Name",
            "Planned Service Type Description",
            "Planned Service Requirement Type Description",
            "Service Requirement Start Date And Time",
            "Service Requirement End Date And Time",
            "Service Requirement Duration",
            "Actual Start Date And Time",
            "Actual End Date And Time",
        ])
        ws.append([
            "Smith, Jane",
            "Personal Care",
            "Personal Care",
            None,
            None,
            0.75,
            datetime(2026, 7, 16, 9, 0),
            datetime(2026, 7, 16, 9, 45),
        ])
        ws.append([
            "Smith, Jane",
            "Personal Care",
            "Personal Care",
            None,
            None,
            0.75,
            None,
            None,
        ])
        tmp = Path(tempfile.mkdtemp()) / "fallback.xlsx"
        wb.save(tmp)
        wb.close()
        try:
            clients_map = {normalize_name_for_match("Smith, Jane"): 42}
            records, unmatched = process_xlsx_file(tmp, clients_map)
            self.assertEqual(unmatched, [])
            self.assertEqual(len(records[42]), 1)
            rec = records[42][0]
            self.assertEqual(rec["start_date"], date(2026, 7, 16))
            self.assertEqual(rec["start_time"].strftime("%H:%M"), "09:00")
            self.assertEqual(rec["end_time"].strftime("%H:%M"), "09:45")
        finally:
            tmp.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
