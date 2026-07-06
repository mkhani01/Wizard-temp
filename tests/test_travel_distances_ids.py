#!/usr/bin/env python3
"""Tests for travel_distances migration SQL type handling."""

import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_MODULE_PATH = PROJECT_ROOT / "distance_migration" / "travel_distances_migration.py"
_SPEC = importlib.util.spec_from_file_location("travel_distances_migration", _MODULE_PATH)
tdm = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(tdm)

# Sample IDs from production exceed PostgreSQL int4 max (2_147_483_647).
LARGE_USER_ID = 20260706000460
LARGE_CLIENT_ID = 120260706000002
INT32_MAX = 2_147_483_647

KNOWN_ENUM_CASTS = {
    "from_type": "::travel_distances_from_type_enum",
    "to_type": "::travel_distances_to_type_enum",
    "travel_method": "::travel_distances_travel_method_enum",
    "calculation_status": "::travel_distances_calculation_status_enum",
}


def _sample_row():
    now = datetime.utcnow()
    return (
        "user", LARGE_USER_ID, "client", LARGE_CLIENT_ID, "car",
        1000, 5, "completed", None, now, now, now,
    )


class TestTravelDistancesSqlTypes(unittest.TestCase):
    def setUp(self):
        tdm._TD_ENUM_CASTS_CACHE = None

    def tearDown(self):
        tdm._TD_ENUM_CASTS_CACHE = None

    def test_get_existing_pairs_uses_bigint_arrays(self):
        connection = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        connection.cursor.return_value = cursor

        pair_keys = {(LARGE_USER_ID, LARGE_CLIENT_ID)}
        tdm.get_existing_pairs_for_keys(
            connection, "user", "client", "car", pair_keys,
        )

        sql = cursor.execute.call_args[0][0]
        self.assertIn("bigint[]", sql)
        self.assertNotIn("int[]", sql)

        args = cursor.execute.call_args[0][1]
        from_ids, to_ids = args[3], args[4]
        self.assertEqual(from_ids, [LARGE_USER_ID])
        self.assertEqual(to_ids, [LARGE_CLIENT_ID])
        self.assertGreater(LARGE_USER_ID, INT32_MAX)
        self.assertGreater(LARGE_CLIENT_ID, INT32_MAX)

    @patch.object(tdm, "_resolve_travel_distances_enum_casts", return_value=KNOWN_ENUM_CASTS)
    def test_copy_insert_casts_enum_columns(self, _mock_casts):
        cursor = MagicMock()
        cursor.copy_expert = MagicMock()

        tdm._copy_insert_batch(cursor, [_sample_row()])

        insert_sql = cursor.execute.call_args_list[-1][0][0]
        self.assertIn("from_type::travel_distances_from_type_enum", insert_sql)
        self.assertIn("to_type::travel_distances_to_type_enum", insert_sql)
        self.assertIn("travel_method::travel_distances_travel_method_enum", insert_sql)
        self.assertIn(
            "calculation_status::travel_distances_calculation_status_enum",
            insert_sql,
        )

        create_sql = cursor.execute.call_args_list[0][0][0]
        self.assertIn("from_id bigint", create_sql)
        self.assertIn("to_id bigint", create_sql)

    @patch.object(tdm, "_resolve_travel_distances_enum_casts", return_value=KNOWN_ENUM_CASTS)
    def test_insert_batch_casts_enum_placeholders(self, _mock_casts):
        cursor = MagicMock()
        with patch.object(tdm, "execute_values") as mock_execute_values:
            tdm.insert_batch(cursor, [_sample_row()])

        template = mock_execute_values.call_args.kwargs["template"]
        self.assertIn("%s::travel_distances_from_type_enum", template)
        self.assertIn("%s::travel_distances_to_type_enum", template)
        self.assertIn("%s::travel_distances_travel_method_enum", template)
        self.assertIn("%s::travel_distances_calculation_status_enum", template)

    def test_resolve_enum_casts_uses_catalog_when_available(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            {"attname": "from_type", "typname": "travel_distances_from_type_enum"},
            {"attname": "to_type", "typname": "travel_distances_to_type_enum"},
            {"attname": "travel_method", "typname": "travel_distances_travel_method_enum"},
            {"attname": "calculation_status", "typname": "travel_distances_calculation_status_enum"},
        ]

        casts = tdm._resolve_travel_distances_enum_casts(cursor)
        self.assertEqual(casts["from_type"], "::travel_distances_from_type_enum")
        self.assertEqual(casts["to_type"], "::travel_distances_to_type_enum")
        self.assertEqual(casts["travel_method"], "::travel_distances_travel_method_enum")
        self.assertEqual(
            casts["calculation_status"],
            "::travel_distances_calculation_status_enum",
        )


if __name__ == "__main__":
    unittest.main()
