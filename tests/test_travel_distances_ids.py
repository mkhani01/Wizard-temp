#!/usr/bin/env python3
"""Tests for travel_distances migration ID handling (bigint entity IDs)."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from distance_migration.travel_distances_migration import (
    _copy_insert_batch,
    get_existing_pairs_for_keys,
)

# Sample IDs from production exceed PostgreSQL int4 max (2_147_483_647).
LARGE_USER_ID = 20260706000460
LARGE_CLIENT_ID = 120260706000002
INT32_MAX = 2_147_483_647


class TestTravelDistancesBigintIds(unittest.TestCase):
    def test_get_existing_pairs_uses_bigint_arrays(self):
        connection = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        connection.cursor.return_value = cursor

        pair_keys = {(LARGE_USER_ID, LARGE_CLIENT_ID)}
        get_existing_pairs_for_keys(
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

    def test_copy_insert_stage_table_uses_bigint_ids(self):
        cursor = MagicMock()
        _copy_insert_batch(cursor, [])

        create_sql = cursor.execute.call_args_list[0][0][0]
        self.assertIn("from_id bigint", create_sql)
        self.assertIn("to_id bigint", create_sql)
        self.assertNotRegex(create_sql, r"\bfrom_id int\b")
        self.assertNotRegex(create_sql, r"\bto_id int\b")


if __name__ == "__main__":
    unittest.main()
