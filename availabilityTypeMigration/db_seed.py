"""
Database seed for Availability Types migration.
Inserts/updates availability_types table to match NestJS entity:
id, name, type, category, description, color, icon, is_paid, created_date, last_modified_date, deleted_at.

Handles multiple DB constraint shapes:
- UNIQUE (name, type, category): single upsert.
- No such constraint: insert new + update by (name, type, category).
- UNIQUE on name (e.g. uq_unavailability_types_name_active): one row per name, insert/update by name.
"""

import logging
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from .csv_parser import deduplicate

logger = logging.getLogger(__name__)


def _normalize_name(name):
    return (name or "").strip().lower()


def _seed_with_upsert(cursor, connection, unique_types, now):
    """Use case-insensitive name matching: update existing row if (name_lower, type, category) exists, else insert."""
    cursor.execute(
        "SELECT id, name, type, category FROM availability_types WHERE deleted_at IS NULL"
    )
    existing = {
        (_normalize_name(r["name"]), r["type"], r["category"]): r["id"]
        for r in cursor.fetchall()
    }
    to_insert = []
    to_update = []
    for t in unique_types:
        key_lower = (_normalize_name(t["name"]), t["type"], t["category"])
        if key_lower in existing:
            to_update.append((t, existing[key_lower]))
        else:
            to_insert.append(t)

    processed = []
    if to_insert:
        insert_query = """
            INSERT INTO availability_types (
                name, type, category, description, color, icon, is_paid,
                created_date, last_modified_date
            ) VALUES %s
            RETURNING id, name, type, category, is_paid
        """
        type_tuples = [
            (
                t["name"], t["type"], t["category"], t["description"],
                t["color"], t["icon"], t["is_paid"], now, now,
            )
            for t in to_insert
        ]
        execute_values(
            cursor, insert_query, type_tuples,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        )
        processed.extend(cursor.fetchall())

    for t, pk in to_update:
        cursor.execute(
            """
            UPDATE availability_types SET
                description = %s, color = %s, icon = %s, is_paid = %s,
                last_modified_date = %s
            WHERE id = %s
            """,
            (t["description"], t["color"], t["icon"], t["is_paid"], now, pk),
        )
        if cursor.rowcount:
            processed.append({
                "id": pk, "name": t["name"], "type": t["type"],
                "category": t["category"], "is_paid": t["is_paid"],
            })
    return processed


def _seed_without_upsert(cursor, connection, unique_types, now):
    """
    Insert new rows and update existing by (name, type, category).
    Name comparison is case-insensitive (Core and core are the same).
    """
    cursor.execute(
        "SELECT id, name, type, category FROM availability_types WHERE deleted_at IS NULL"
    )
    existing = {
        (_normalize_name(r["name"]), r["type"], r["category"]): r["id"]
        for r in cursor.fetchall()
    }

    to_insert = []
    to_update = []
    for t in unique_types:
        key = (_normalize_name(t["name"]), t["type"], t["category"])
        if key in existing:
            to_update.append((t, existing[key]))
        else:
            to_insert.append(t)

    processed = []

    if to_insert:
        insert_query = """
            INSERT INTO availability_types (
                name, type, category, description, color, icon, is_paid,
                created_date, last_modified_date
            ) VALUES %s
            RETURNING id, name, type, category, is_paid
        """
        type_tuples = [
            (
                t["name"], t["type"], t["category"], t["description"],
                t["color"], t["icon"], t["is_paid"], now, now,
            )
            for t in to_insert
        ]
        execute_values(
            cursor, insert_query, type_tuples,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        )
        processed.extend(cursor.fetchall())

    for t, pk in to_update:
        cursor.execute(
            """
            UPDATE availability_types SET
                description = %s, color = %s, icon = %s, is_paid = %s,
                last_modified_date = %s
            WHERE id = %s
            """,
            (t["description"], t["color"], t["icon"], t["is_paid"], now, pk),
        )
        if cursor.rowcount:
            processed.append({
                "id": pk, "name": t["name"], "type": t["type"],
                "category": t["category"], "is_paid": t["is_paid"],
            })

    return processed


def _deduplicate_by_name(types):
    """Keep first occurrence per name (case-insensitive; for DBs with UNIQUE on name only)."""
    seen = {}
    for t in types:
        name = t.get("name")
        if name is not None:
            key = _normalize_name(name)
            if key not in seen:
                seen[key] = t
    return list(seen.values())


def _seed_by_name_only(cursor, connection, unique_types, now):
    """
    When table has UNIQUE on (name) only (e.g. uq_unavailability_types_name_active):
    one row per name; deduplicate by name then insert new and update existing.
    """
    types_by_name = _deduplicate_by_name(unique_types)
    if len(types_by_name) < len(unique_types):
        logger.info(
            "Table has one row per name; reduced %s -> %s records by name",
            len(unique_types), len(types_by_name),
        )

    cursor.execute(
        "SELECT id, name FROM availability_types WHERE deleted_at IS NULL"
    )
    existing_by_name = {_normalize_name(r["name"]): r["id"] for r in cursor.fetchall()}

    to_insert = [t for t in types_by_name if _normalize_name(t["name"]) not in existing_by_name]
    to_update = [(t, existing_by_name[_normalize_name(t["name"])]) for t in types_by_name if _normalize_name(t["name"]) in existing_by_name]

    processed = []

    if to_insert:
        insert_query = """
            INSERT INTO availability_types (
                name, type, category, description, color, icon, is_paid,
                created_date, last_modified_date
            ) VALUES %s
            RETURNING id, name, type, category, is_paid
        """
        type_tuples = [
            (
                t["name"], t["type"], t["category"], t["description"],
                t["color"], t["icon"], t["is_paid"], now, now,
            )
            for t in to_insert
        ]
        execute_values(
            cursor, insert_query, type_tuples,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        )
        processed.extend(cursor.fetchall())

    for t, pk in to_update:
        cursor.execute(
            """
            UPDATE availability_types SET
                type = %s, category = %s, description = %s, color = %s,
                icon = %s, is_paid = %s, last_modified_date = %s
            WHERE id = %s
            """,
            (t["type"], t["category"], t["description"], t["color"],
            t["icon"], t["is_paid"], now, pk),
        )
        if cursor.rowcount:
            processed.append({
                "id": pk, "name": t["name"], "type": t["type"],
                "category": t["category"], "is_paid": t["is_paid"],
            })

    return processed


def seed_availability_types(connection, types):
    """
    Insert or update availability_types records.

    Deduplicates by (name, type, category). Tries ON CONFLICT upsert first;
    if the table has no unique constraint on (name, type, category), falls
    back to insert-new + update-existing.
    """
    if not types:
        logger.warning("No availability types to insert")
        return False

    unique_types = deduplicate(types)
    logger.info("Inserting/updating %s availability types...", len(unique_types))

    cursor = connection.cursor()
    try:
        now = datetime.now()
        try:
            processed = _seed_with_upsert(cursor, connection, unique_types, now)
        except Exception as e:
            err_str = str(e)
            if "no unique or exclusion constraint matching the ON CONFLICT" in err_str:
                connection.rollback()
                logger.info(
                    "Table has no UNIQUE (name, type, category); using insert/update fallback"
                )
                processed = _seed_without_upsert(cursor, connection, unique_types, now)
            elif (
                isinstance(e, psycopg2.IntegrityError)
                and getattr(e, "pgcode", None) == "23505"
            ) or "uq_unavailability_types_name_active" in err_str or "duplicate key value violates unique constraint" in err_str:
                connection.rollback()
                logger.info(
                    "Table has UNIQUE on name (e.g. uq_unavailability_types_name_active); using one row per name"
                )
                processed = _seed_by_name_only(cursor, connection, unique_types, now)
            else:
                raise

        connection.commit()

        logger.info("✓ Successfully processed %s availability types", len(processed))
        logger.info("Sample records:")
        for row in processed[:5]:
            logger.info(
                "  - %s (type=%s, category=%s, is_paid=%s, id=%s)",
                row["name"], row["type"], row["category"], row["is_paid"], row["id"],
            )
        if len(processed) > 5:
            logger.info("  ... and %s more", len(processed) - 5)

        return True

    except Exception as e:
        connection.rollback()
        logger.error("✗ Failed to insert availability types: %s", e)
        raise
    finally:
        cursor.close()
