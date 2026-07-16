"""
Build scoped pair sets for travel distance migration.

Instead of full N×M matrices, only pairs needed by engine, profile prefs, and
carer travel limits (route-derived client↔client legs) are computed.
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

logger = logging.getLogger(__name__)

PairKey = Tuple[str, str]  # (from_type, to_type)
PairSet = Set[Tuple[int, int]]


def _load_feasible_pairs(connection) -> Set[Tuple[int, int]]:
    """(user_id, client_id) from feasible_pairs."""
    pairs: Set[Tuple[int, int]] = set()
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT cgid, client_id FROM feasible_pairs")
        for row in cursor.fetchall():
            pairs.add((int(row["cgid"]), int(row["client_id"])))
    finally:
        cursor.close()
    return pairs


def _load_profile_pairs(connection) -> Set[Tuple[int, int]]:
    try:
        from feasible_pairs_migration.profile_preferences import load_profile_user_client_pairs
        return load_profile_user_client_pairs(connection)
    except Exception as e:
        logger.warning("Could not load profile pairs: %s", e)
        return set()


def _load_route_client_pairs(connection, visit_csv_path: Optional[Path]) -> Set[Tuple[int, int]]:
    """Consecutive client↔client pairs from VisitExport daily routes."""
    if not visit_csv_path or not Path(visit_csv_path).exists():
        return set()

    try:
        from carerTravelLimitsMigration.main import extract_daily_routes_from_csv
        from feasible_pairs_migration.feasible_pairs_migration import (
            load_clients_lookup,
            load_users_lookup,
        )
    except ImportError as e:
        logger.warning("Could not import route helpers: %s", e)
        return set()

    try:
        users_lookup = load_users_lookup(connection)
        clients_lookup = load_clients_lookup(connection)
    except Exception as e:
        logger.warning("Could not load lookups for route pairs: %s", e)
        return set()

    daily_routes, _stats = extract_daily_routes_from_csv(
        visit_csv_path, users_lookup, clients_lookup,
    )
    pairs: Set[Tuple[int, int]] = set()
    for _route_key, client_ids in daily_routes.items():
        for prev_client, next_client in zip(client_ids, client_ids[1:]):
            if prev_client != next_client:
                pairs.add((prev_client, next_client))
    return pairs


def build_required_pairs(
    connection,
    visit_csv_path=None,
) -> Dict[PairKey, PairSet]:
    """
    Returns pair sets keyed by (from_type, to_type):
      - ("user", "client"): (user_id, client_id)
      - ("client", "user"): (client_id, user_id)
      - ("client", "client"): (client_id_a, client_id_b)
    """
    user_client: Set[Tuple[int, int]] = set()
    client_user: Set[Tuple[int, int]] = set()
    client_client: Set[Tuple[int, int]] = set()

    feasible = _load_feasible_pairs(connection)
    profile = _load_profile_pairs(connection)
    all_uc = feasible | profile

    for user_id, client_id in all_uc:
        user_client.add((user_id, client_id))
        client_user.add((client_id, user_id))

    route_cc = _load_route_client_pairs(
        connection,
        Path(visit_csv_path) if visit_csv_path else None,
    )
    client_client.update(route_cc)

    result = {
        ("user", "client"): user_client,
        ("client", "user"): client_user,
        ("client", "client"): client_client,
    }
    logger.info(
        "Scoped pair counts: user→client=%d, client→user=%d, client→client=%d",
        len(user_client),
        len(client_user),
        len(client_client),
    )
    return result


def build_full_matrix_pairs(
    user_ids: Set[int],
    client_ids: Set[int],
) -> Dict[PairKey, PairSet]:
    """Legacy full-matrix pair sets for DISTANCE_MODE=full."""
    user_user = {(u1, u2) for u1 in user_ids for u2 in user_ids}
    client_client = {(c1, c2) for c1 in client_ids for c2 in client_ids}
    user_client = {(u, c) for u in user_ids for c in client_ids}
    client_user = {(c, u) for c in client_ids for u in user_ids}
    result = {
        ("user", "user"): user_user,
        ("client", "client"): client_client,
        ("user", "client"): user_client,
        ("client", "user"): client_user,
    }
    logger.info(
        "Full matrix pair counts: user→user=%d, client→client=%d, user→client=%d, client→user=%d",
        len(user_user),
        len(client_client),
        len(user_client),
        len(client_user),
    )
    return result


def get_distance_mode() -> str:
    import os
    return os.getenv("DISTANCE_MODE", "full").strip().lower()


def resolve_visit_csv_path(explicit_path=None) -> Optional[Path]:
    if explicit_path:
        p = Path(explicit_path)
        return p if p.exists() else None
    import os
    env_path = os.getenv("VISIT_EXPORT_CSV")
    if env_path and Path(env_path).exists():
        return Path(env_path)
    try:
        from migration_support import get_assets_dir
        default = get_assets_dir() / "visit_data.csv"
        return default if default.exists() else None
    except ImportError:
        return None
