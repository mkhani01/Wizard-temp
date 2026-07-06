"""Unit tests for profile Must/Preferred/Only classification."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from feasible_pairs_migration.profile_preferences import (
    LONG_DURATION_MINUTES,
    WEIGHT_THRESHOLD,
    classify_pairs,
    classify_profile_category,
    build_profile_rows,
)


def test_classify_only_for_long_duration_high_weight():
    assert classify_profile_category(0.95, LONG_DURATION_MINUTES, None) == "only"
    assert classify_profile_category(0.95, LONG_DURATION_MINUTES + 60, "Support / Relief") == "only"


def test_classify_must_for_normal_duration_high_weight():
    assert classify_profile_category(0.95, LONG_DURATION_MINUTES - 1, None) == "must"
    assert classify_profile_category(WEIGHT_THRESHOLD, 60, None) == "must"


def test_classify_preferred_for_current_primary():
    assert classify_profile_category(0.5, 60, "Current Primary") == "preferred"
    assert classify_profile_category(0.89, 400, "Current Primary") == "preferred"


def test_must_only_take_precedence_over_preferred():
    categorized = classify_pairs(
        weights={(1, 10): 0.95, (2, 20): 0.5},
        statuses={(2, 20): "Current Primary"},
        client_durations={10: 400, 20: 60},
    )
    assert len(categorized["only"]) == 1
    assert categorized["only"][0][:2] == (1, 10)
    assert len(categorized["must"]) == 0
    assert len(categorized["preferred"]) == 1
    assert categorized["preferred"][0][:2] == (2, 20)


def test_exclusivity_in_build_profile_rows():
    categorized = classify_pairs(
        weights={(1, 10): 0.95, (1, 11): 0.5},
        statuses={(1, 11): "Current Primary"},
        client_durations={10: 60, 11: 60},
    )
    rows = build_profile_rows(categorized)
    user_must = {(r[0], r[1]) for r in rows["user_must_clients"]}
    user_pref = {(r[0], r[1]) for r in rows["user_preferred_clients"]}
    assert (1, 10) in user_must
    assert (1, 11) in user_pref
    assert user_must & user_pref == set()


def test_two_way_sync_row_counts():
    categorized = classify_pairs(
        weights={(5, 100): 0.92},
        statuses={},
        client_durations={100: 500},
    )
    rows = build_profile_rows(categorized)
    assert len(rows["user_only_clients"]) == len(rows["client_only_users"]) == 1
    uid, cid, so_user = rows["user_only_clients"][0]
    cid2, uid2, so_client = rows["client_only_users"][0]
    assert uid == uid2 == 5
    assert cid == cid2 == 100


if __name__ == "__main__":
    test_classify_only_for_long_duration_high_weight()
    test_classify_must_for_normal_duration_high_weight()
    test_classify_preferred_for_current_primary()
    test_must_only_take_precedence_over_preferred()
    test_exclusivity_in_build_profile_rows()
    test_two_way_sync_row_counts()
    print("All profile preference tests passed.")
