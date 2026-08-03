"""Unit tests for profile Must/Preferred/Only classification."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from feasible_pairs_migration.profile_preferences import (
    LONG_DURATION_MINUTES,
    WEIGHT_THRESHOLD,
    classify_pairs,
    classify_profile_category,
    build_profile_rows,
    sync_extended_feasibility,
)


def test_classify_only_for_long_duration_high_weight():
    assert classify_profile_category(1.0, LONG_DURATION_MINUTES, None) == "only"
    assert classify_profile_category(1.0, LONG_DURATION_MINUTES + 60, "Support / Relief") == "only"


def test_classify_must_for_normal_duration_high_weight():
    assert classify_profile_category(1.0, LONG_DURATION_MINUTES - 1, None) == "must"
    assert classify_profile_category(WEIGHT_THRESHOLD, 60, None) == "must"


def test_weight_below_threshold_not_must_or_only():
    assert classify_profile_category(0.95, LONG_DURATION_MINUTES, None) is None
    assert classify_profile_category(0.95, 60, None) is None


def test_classify_preferred_for_current_primary():
    assert classify_profile_category(0.5, 60, "Current Primary") == "preferred"
    assert classify_profile_category(0.95, 400, "Current Primary") == "preferred"


def test_must_only_take_precedence_over_preferred():
    categorized = classify_pairs(
        weights={(1, 10): 1.0, (2, 20): 0.5},
        statuses={(2, 20): "Current Primary"},
        client_durations={10: 500, 20: 60},
    )
    assert len(categorized["only"]) == 1
    assert categorized["only"][0][:2] == (1, 10)
    assert len(categorized["must"]) == 0
    assert len(categorized["preferred"]) == 1
    assert categorized["preferred"][0][:2] == (2, 20)


def test_exclusivity_in_build_profile_rows():
    categorized = classify_pairs(
        weights={(1, 10): 1.0, (1, 11): 0.5},
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
        weights={(5, 100): 1.0},
        statuses={},
        client_durations={100: 500},
    )
    rows = build_profile_rows(categorized)
    assert len(rows["user_only_clients"]) == len(rows["client_only_users"]) == 1
    uid, cid, so_user = rows["user_only_clients"][0]
    cid2, uid2, so_client = rows["client_only_users"][0]
    assert uid == uid2 == 5
    assert cid == cid2 == 100


def test_only_entities_get_extended_feasibility_false_others_true():
    """Only join membership implies extended_feasibility=false; Must/Preferred do not."""
    categorized = classify_pairs(
        weights={(1, 10): 1.0, (2, 20): 1.0, (3, 30): 0.5},
        statuses={(3, 30): "Current Primary"},
        client_durations={10: 500, 20: 60, 30: 60},
    )
    rows = build_profile_rows(categorized)
    only_users = {r[0] for r in rows["user_only_clients"]}
    only_clients = {r[0] for r in rows["client_only_users"]}
    must_users = {r[0] for r in rows["user_must_clients"]}
    preferred_users = {r[0] for r in rows["user_preferred_clients"]}

    assert only_users == {1}
    assert only_clients == {10}
    assert 2 in must_users
    assert 3 in preferred_users
    # Rule mirrored by sync_extended_feasibility SQL: Only → false, else true
    for uid in must_users | preferred_users:
        assert uid not in only_users  # would get extended_feasibility=true
    for uid in only_users:
        assert uid not in must_users and uid not in preferred_users


def test_sync_extended_feasibility_sql_and_counts():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        {"n_true": 10, "n_false": 2},
        {"n_true": 8, "n_false": 1},
    ]
    result = sync_extended_feasibility(cursor)
    assert result == {
        "user_true": 10,
        "user_false": 2,
        "client_true": 8,
        "client_false": 1,
    }
    sqls = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("user_only_clients" in sql and "extended_feasibility" in sql for sql in sqls)
    assert any("client_only_users" in sql and "extended_feasibility" in sql for sql in sqls)
    assert any('UPDATE "user"' in sql for sql in sqls)
    assert any('UPDATE "client"' in sql for sql in sqls)


if __name__ == "__main__":
    test_classify_only_for_long_duration_high_weight()
    test_classify_must_for_normal_duration_high_weight()
    test_weight_below_threshold_not_must_or_only()
    test_classify_preferred_for_current_primary()
    test_must_only_take_precedence_over_preferred()
    test_exclusivity_in_build_profile_rows()
    test_two_way_sync_row_counts()
    test_only_entities_get_extended_feasibility_false_others_true()
    test_sync_extended_feasibility_sql_and_counts()
    print("All profile preference tests passed.")
