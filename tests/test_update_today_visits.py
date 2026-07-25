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
    extract_temp_visit_rows,
    create_temp_schedules_and_visits,
    _is_empty_datetime_val,
    offset_days_between,
    requested_duration_minutes,
    TERMINATED_CANCELLATION_TYPE,
    PERSONAL_CARE,
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
            clients_map = {
                normalize_name_for_match("Smith, Jane"): [
                    {
                        "id": 42,
                        "name": "Jane",
                        "lastname": "Smith",
                        "status": "Active",
                        "postcode": None,
                    }
                ]
            }
            records, unmatched = process_xlsx_file(tmp, clients_map)
            self.assertEqual(unmatched, [])
            self.assertEqual(len(records[42]), 1)
            rec = records[42][0]
            self.assertEqual(rec["start_date"], date(2026, 7, 16))
            self.assertEqual(rec["start_time"].strftime("%H:%M"), "09:00")
            self.assertEqual(rec["end_time"].strftime("%H:%M"), "09:45")
        finally:
            tmp.unlink(missing_ok=True)


class TestTempVisitHelpers(unittest.TestCase):
    def test_is_empty_datetime_val(self):
        self.assertTrue(_is_empty_datetime_val(None))
        self.assertTrue(_is_empty_datetime_val(""))
        self.assertTrue(_is_empty_datetime_val("   "))
        self.assertFalse(_is_empty_datetime_val("16-07-2026 09:00:00"))
        self.assertFalse(_is_empty_datetime_val(datetime(2026, 7, 16, 9, 0)))

    def test_offset_and_duration_same_day(self):
        start = datetime(2026, 7, 16, 9, 0)
        end = datetime(2026, 7, 16, 9, 45)
        self.assertEqual(offset_days_between(start, end), 0)
        self.assertEqual(requested_duration_minutes(start, end, 0), 45)

    def test_offset_overnight(self):
        start = datetime(2026, 7, 16, 22, 0)
        end = datetime(2026, 7, 17, 6, 0)
        self.assertEqual(offset_days_between(start, end), 1)
        self.assertEqual(requested_duration_minutes(start, end, 1), 8 * 60)


class TestExtractTempVisitRows(unittest.TestCase):
    def _write_workbook(self, rows):
        import tempfile
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Data"
        ws.append(
            [
                "Service Location Name",
                "Service Location Address",
                "Planned Service Type Description",
                "Planned Service Requirement Type Description",
                "Service Requirement Start Date And Time",
                "Service Requirement End Date And Time",
                "Actual Start Date And Time",
                "Actual End Date And Time",
                "Cancellation Description",
            ]
        )
        for row in rows:
            ws.append(row)
        tmp = Path(tempfile.mkdtemp()) / "temp_visits.xlsx"
        wb.save(tmp)
        wb.close()
        return tmp

    def test_extracts_personal_care_with_empty_requirement(self):
        from encoding_utils import normalize_name_for_match

        tmp = self._write_workbook(
            [
                [
                    "Smith, Jane",
                    "1 Main St D01ABCD",
                    "Personal Care",
                    "Personal Care",
                    None,
                    None,
                    datetime(2026, 7, 16, 9, 0),
                    datetime(2026, 7, 16, 9, 45),
                    None,
                ],
                # Requirement filled → skip
                [
                    "Smith, Jane",
                    "1 Main St D01ABCD",
                    "Personal Care",
                    "Personal Care",
                    datetime(2026, 7, 16, 10, 0),
                    datetime(2026, 7, 16, 10, 30),
                    datetime(2026, 7, 16, 10, 0),
                    datetime(2026, 7, 16, 10, 30),
                    None,
                ],
                # Not Personal Care → skip
                [
                    "Smith, Jane",
                    "1 Main St D01ABCD",
                    "Nursing",
                    "Nursing",
                    None,
                    None,
                    datetime(2026, 7, 16, 11, 0),
                    datetime(2026, 7, 16, 11, 30),
                    None,
                ],
                # Has cancellation → skip
                [
                    "Smith, Jane",
                    "1 Main St D01ABCD",
                    "Personal Care",
                    "Personal Care",
                    None,
                    None,
                    datetime(2026, 7, 16, 12, 0),
                    datetime(2026, 7, 16, 12, 30),
                    "Hospital",
                ],
                # Wrong date → skip
                [
                    "Smith, Jane",
                    "1 Main St D01ABCD",
                    "Personal Care",
                    "Personal Care",
                    None,
                    None,
                    datetime(2026, 7, 17, 9, 0),
                    datetime(2026, 7, 17, 9, 45),
                    None,
                ],
            ]
        )
        try:
            clients_map = {
                normalize_name_for_match("Smith, Jane"): [
                    {
                        "id": 42,
                        "name": "Jane",
                        "lastname": "Smith",
                        "status": "Active",
                        "postcode": "D01ABCD",
                    }
                ]
            }
            rows, stats = extract_temp_visit_rows(tmp, date(2026, 7, 16), clients_map)
            self.assertEqual(len(rows), 1)
            self.assertEqual(stats["candidates"], 1)
            self.assertEqual(stats["skipped_req_not_both_empty"], 1)
            self.assertEqual(stats["skipped_not_personal_care"], 1)
            self.assertEqual(stats["skipped_has_cancellation"], 1)
            self.assertEqual(stats["skipped_wrong_date"], 1)
            rec = rows[0]
            self.assertEqual(rec["client_id"], 42)
            self.assertEqual(rec["start_minute"], 540)
            self.assertEqual(rec["end_minute"], 585)
            self.assertEqual(rec["requested_start_time"], "09:00:00")
            self.assertEqual(rec["requested_end_time"], "09:45:00")
            self.assertEqual(rec["requested_duration"], 45)
            self.assertEqual(rec["day_of_week"], "Thursday")
            self.assertEqual(PERSONAL_CARE, "Personal Care")
        finally:
            tmp.unlink(missing_ok=True)

    def test_skips_missing_actual(self):
        from encoding_utils import normalize_name_for_match

        tmp = self._write_workbook(
            [
                [
                    "Smith, Jane",
                    None,
                    "Personal Care",
                    "Personal Care",
                    None,
                    None,
                    None,
                    None,
                    None,
                ]
            ]
        )
        try:
            clients_map = {
                normalize_name_for_match("Smith, Jane"): [
                    {
                        "id": 42,
                        "name": "Jane",
                        "lastname": "Smith",
                        "status": "Active",
                        "postcode": None,
                    }
                ]
            }
            rows, stats = extract_temp_visit_rows(tmp, date(2026, 7, 16), clients_map)
            self.assertEqual(rows, [])
            self.assertEqual(stats["skipped_missing_actual"], 1)
        finally:
            tmp.unlink(missing_ok=True)


class TestCreateTempSchedulesAndVisits(unittest.TestCase):
    """
    Regression coverage for server/src client-schedule.helpers.ts behaviour:
    a temporary client_schedule's absolute window is capped at
    preferences.effective_date_to (capAbsoluteWindowAtDate /
    resolveClientScheduleVisitWindowForDate — see
    schedule.helpers.spec.ts "caps temporary schedules at effectiveDateTo").
    For overnight/multi-day visits (end_time_date_offset_days > 0), end_date
    and effective_date_to must therefore be target_date + offset_days, not
    target_date, or the continuation-day portion of the visit is dropped by
    the server.
    """

    def _run(self, candidate, target_date=date(2026, 7, 16)):
        connection = MagicMock()
        cursor = MagicMock()
        connection.cursor.return_value = cursor
        cursor.fetchone.side_effect = [
            None,  # enum type lookup -> no client_schedules_days_enum
            None,  # _find_existing_temp_schedule_id -> no existing schedule
            {"id": 501},  # INSERT client_schedules ... RETURNING id
            None,  # _active_visit_exists (client_schedule_id branch)
            None,  # _active_visit_exists (fallback branch)
        ]

        counts = create_temp_schedules_and_visits(
            connection,
            roster_id="roster-1",
            target_date=target_date,
            candidates=[candidate],
            terminated_ids=set(),
            personal_care_service_type_id=None,
        )

        schedule_call = next(
            c
            for c in cursor.execute.call_args_list
            if "INSERT INTO client_schedules (" in c.args[0]
        )
        prefs_call = next(
            c
            for c in cursor.execute.call_args_list
            if "INSERT INTO client_schedule_preferences (" in c.args[0]
        )
        return counts, schedule_call.args[1], prefs_call.args[1]

    def test_overnight_visit_extends_end_date_and_effective_date_to(self):
        candidate = {
            "row_num": 5,
            "client_id": 42,
            "client_name": "Smith, Jane",
            "start_minute": 22 * 60,
            "end_minute": 6 * 60,
            "requested_start_time": "22:00:00",
            "requested_end_time": "06:00:00",
            "requested_duration": 8 * 60,
            "end_time_date_offset_days": 1,
            "day_of_week": "Thursday",
        }
        counts, schedule_params, prefs_params = self._run(candidate)

        self.assertEqual(counts["created_schedules"], 1)
        self.assertEqual(counts["created_visits"], 1)

        # client_schedules: (..., requested_duration, start_date, end_date,
        # occurs_every, number_of_care_givers, end_time_date_offset_days)
        self.assertEqual(schedule_params[5], "2026-07-16")  # start_date
        self.assertEqual(schedule_params[6], "2026-07-17")  # end_date = start + offset
        self.assertEqual(schedule_params[9], 1)  # end_time_date_offset_days

        # client_schedule_preferences: (..., effective_date_from,
        # effective_date_to, note)
        self.assertEqual(prefs_params[5], "2026-07-16")  # effective_date_from
        self.assertEqual(
            prefs_params[6], "2026-07-17"
        )  # effective_date_to must match end_date, not start_date

    def test_same_day_visit_keeps_effective_date_to_on_target_date(self):
        candidate = {
            "row_num": 6,
            "client_id": 43,
            "client_name": "Doe, Jane",
            "start_minute": 9 * 60,
            "end_minute": 9 * 60 + 45,
            "requested_start_time": "09:00:00",
            "requested_end_time": "09:45:00",
            "requested_duration": 45,
            "end_time_date_offset_days": 0,
            "day_of_week": "Thursday",
        }
        counts, schedule_params, prefs_params = self._run(candidate)

        self.assertEqual(counts["created_schedules"], 1)
        self.assertEqual(schedule_params[5], "2026-07-16")
        self.assertEqual(schedule_params[6], "2026-07-16")
        self.assertEqual(prefs_params[5], "2026-07-16")
        self.assertEqual(prefs_params[6], "2026-07-16")


if __name__ == "__main__":
    unittest.main()
