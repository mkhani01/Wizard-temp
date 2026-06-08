"""Unit tests for Patient_Analyzer and feasible-pairs analytics helpers."""

import unittest
from datetime import datetime, timedelta
from collections import defaultdict

from clientWindowsAnalyzer.main import (
    compute_min_duration_from_suggested,
    get_balanced_suggestion,
)
from feasible_pairs_migration.feasible_pairs_migration import (
    identify_carer_status,
    calculate_pair_weights,
    calculate_pair_statuses,
    is_valid_feasibility_row,
    is_excluded_service_type,
    ROSTER_WINDOW_DAYS,
)


class TestMinDurationFormula(unittest.TestCase):
    def test_sixty_five_percent_when_requested_differs(self):
        self.assertEqual(compute_min_duration_from_suggested(60, 45, 60), 29)

    def test_eighty_five_percent_when_requested_matches(self):
        self.assertEqual(compute_min_duration_from_suggested(45, 45, 60), 38)

    def test_clamped_to_slot_width(self):
        self.assertEqual(compute_min_duration_from_suggested(60, 45, 20), 20)

    def test_minimum_one_minute(self):
        self.assertEqual(compute_min_duration_from_suggested(5, 1, 10), 1)


class TestBalancedSuggestion(unittest.TestCase):
    def test_reduces_to_highest_significant_duration(self):
        dist = {30: 5, 45: 12, 60: 33}
        self.assertEqual(get_balanced_suggestion(60, dist), 45)

    def test_no_reduction_when_below_threshold(self):
        dist = {45: 2, 60: 48}
        self.assertEqual(get_balanced_suggestion(60, dist), 60)

    def test_unchanged_when_only_required_duration(self):
        dist = {30: 47}
        self.assertEqual(get_balanced_suggestion(30, dist), 30)

    def test_empty_distribution(self):
        self.assertEqual(get_balanced_suggestion(60, {}), 60)


class TestFeasiblePairWeights(unittest.TestCase):
    def test_primary_carer_higher_weight_than_former(self):
        dataset_end = datetime(2026, 2, 18)
        client_id = 1
        cg_primary = 10
        cg_former = 11

        frequencies = {(cg_primary, client_id): 40, (cg_former, client_id): 5}
        pair_last_visit = {
            (cg_primary, client_id): dataset_end - timedelta(days=7),
            (cg_former, client_id): dataset_end - timedelta(days=60),
        }
        customer_totals = defaultdict(int, {client_id: 45})

        weights = calculate_pair_weights(
            frequencies, pair_last_visit, customer_totals, dataset_end
        )
        self.assertGreater(weights[(cg_primary, client_id)], weights[(cg_former, client_id)])
        self.assertEqual(weights[(cg_primary, client_id)], 1.0)

    def test_carer_status_mapping(self):
        self.assertEqual(identify_carer_status(50, 10), "Current Primary")
        self.assertEqual(identify_carer_status(30, 10), "Support / Relief")
        self.assertEqual(identify_carer_status(50, 60), "Former / Relief")

    def test_pair_statuses_current_primary(self):
        dataset_end = datetime(2026, 2, 18)
        client_id = 1
        frequencies = {(10, client_id): 40, (11, client_id): 5}
        pair_last_visit = {
            (10, client_id): dataset_end - timedelta(days=7),
            (11, client_id): dataset_end - timedelta(days=60),
        }
        customer_totals = defaultdict(int, {client_id: 45})
        statuses = calculate_pair_statuses(
            frequencies, pair_last_visit, customer_totals, dataset_end,
        )
        self.assertEqual(statuses[(10, client_id)], "Current Primary")
        self.assertEqual(statuses[(11, client_id)], "Former / Relief")


class TestFeasibilityRowFilters(unittest.TestCase):
    def test_excludes_break_time(self):
        row = {"Service Requirement Service Type Description": "Break Time"}
        self.assertTrue(is_excluded_service_type(row))
        self.assertFalse(is_valid_feasibility_row(row))

    def test_accepts_personal_care_visitexport(self):
        row = {
            "Service Requirement Service Type Description": "Personal Care",
            "Actual Employee Name": "Smith, Jane",
        }
        self.assertFalse(is_excluded_service_type(row))
        self.assertTrue(is_valid_feasibility_row(row))

    def test_roster_window_constant(self):
        self.assertEqual(ROSTER_WINDOW_DAYS, 16 * 7)


if __name__ == "__main__":
    unittest.main()
