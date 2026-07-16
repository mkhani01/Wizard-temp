#!/usr/bin/env python3
"""
Run all tests. Use before starting migration to ensure environment and modules are OK.
  python tests/run_tests.py
  python -m tests.run_tests
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tests.test_before_run import run_all as run_pre_run_checks


def main():
    if not run_pre_run_checks():
        return 1
    import unittest
    loader = unittest.defaultTestLoader()
    analytics_suite = loader.loadTestsFromName("tests.test_analytics")
    travel_suite = loader.loadTestsFromName("tests.test_travel_limits")
    combined = unittest.TestSuite([analytics_suite, travel_suite])
    analytics_result = unittest.TextTestRunner(verbosity=1).run(combined)
    if not analytics_result.wasSuccessful():
        return 1
    print("Optional: distance test (requires caregivers.json / patient.json)")
    from tests.test_distance import test_small_example
    result = test_small_example()
    if result is False:
        return 1
    print("Profile preference classification tests...")
    from tests.test_profile_preferences import (
        test_classify_only_for_long_duration_high_weight,
        test_classify_must_for_normal_duration_high_weight,
        test_classify_preferred_for_current_primary,
        test_must_only_take_precedence_over_preferred,
        test_exclusivity_in_build_profile_rows,
        test_two_way_sync_row_counts,
    )
    test_classify_only_for_long_duration_high_weight()
    test_classify_must_for_normal_duration_high_weight()
    test_classify_preferred_for_current_primary()
    test_must_only_take_precedence_over_preferred()
    test_exclusivity_in_build_profile_rows()
    test_two_way_sync_row_counts()
    print("  ✓ Profile preference tests passed")
    print("Update today visits / Actual fallback tests...")
    update_suite = loader.loadTestsFromName("tests.test_update_today_visits")
    update_result = unittest.TextTestRunner(verbosity=1).run(update_suite)
    if not update_result.wasSuccessful():
        return 1
    print("\n✓ All tests OK. You can run the wizard or CLI.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
