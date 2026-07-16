#!/usr/bin/env python3
"""Tests for distance pair scope and completeness expectations."""

import importlib.util
import os
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_PAIR_SCOPE_PATH = PROJECT_ROOT / "distance_migration" / "pair_scope.py"
_SPEC = importlib.util.spec_from_file_location("pair_scope", _PAIR_SCOPE_PATH)
pair_scope = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pair_scope)


class TestPairScope(unittest.TestCase):
    def test_default_distance_mode_is_full(self):
        old = os.environ.pop("DISTANCE_MODE", None)
        try:
            self.assertEqual(pair_scope.get_distance_mode(), "full")
        finally:
            if old is not None:
                os.environ["DISTANCE_MODE"] = old

    def test_full_matrix_pair_counts(self):
        users = {1, 2, 3}
        clients = {10, 20}
        pairs = pair_scope.build_full_matrix_pairs(users, clients)
        self.assertEqual(len(pairs[("user", "user")]), 9)
        self.assertEqual(len(pairs[("client", "client")]), 4)
        self.assertEqual(len(pairs[("user", "client")]), 6)
        self.assertEqual(len(pairs[("client", "user")]), 6)

        total_rows = sum(len(s) for s in pairs.values()) * 3
        self.assertEqual(total_rows, 75)


if __name__ == "__main__":
    unittest.main()
