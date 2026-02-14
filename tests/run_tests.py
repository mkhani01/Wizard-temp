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
    print("Optional: distance test (requires caregivers.json / patient.json)")
    from tests.test_distance import test_small_example
    result = test_small_example()
    if result is False:
        return 1
    print("\n✓ All tests OK. You can run the wizard or CLI.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
