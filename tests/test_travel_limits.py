"""Unit tests for carer travel limits histogram helper."""

import unittest

from carerTravelLimitsMigration.main import modal_histogram_midpoint


class TestModalHistogramMidpoint(unittest.TestCase):
    def test_returns_none_with_insufficient_samples(self):
        self.assertIsNone(modal_histogram_midpoint([1.5]))
        self.assertIsNone(modal_histogram_midpoint([]))

    def test_single_value_range(self):
        self.assertEqual(modal_histogram_midpoint([3.0, 3.0, 3.0]), 3.0)

    def test_picks_modal_bin_midpoint(self):
        values = [1.0, 1.1, 1.2, 9.0, 9.1]
        result = modal_histogram_midpoint(values, num_bins=10)
        self.assertIsNotNone(result)
        self.assertLess(result, 5.0)

    def test_requires_at_least_two_samples(self):
        values = [2.0, 4.0, 4.0, 4.0, 20.0]
        result = modal_histogram_midpoint(values)
        self.assertIsNotNone(result)
        self.assertGreaterEqual(result, 2.0)
        self.assertLessEqual(result, 20.0)


if __name__ == "__main__":
    unittest.main()
