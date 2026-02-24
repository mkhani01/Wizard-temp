"""
Database seed for Availability Types migration.
Inserts/updates availability_types table to match NestJS entity:
id, name, type, category, description, color, icon, is_paid, created_date, last_modified_date, deleted_at.

Match by name only (case-insensitive): if a record with the same name exists, update its
type, category, description, color, icon, is_paid; otherwise insert a new row.
"""

import logging
from datetime import datetime

from psycopg2.extras import execute_values

from .csv_parser import deduplicate

logger = logging.getLogger(__name__)


def _normalize_name(name):
    return (name or "").strip().lower()


def _deduplicate_by_name(types):
    """Keep last occurrence per name (case-insensitive). One row per name for insert/update by name."""
    seen = {}
    for t in types:
        name = t.get("name")
        if name is not None:
            key = _normalize_name(name)
            seen[key] = t
    return list(seen.values())


def _seed_by_name(cursor, types_deduped, now):
    """
    Match by name only (case-insensitive). If name exists → update type, category,
    description, color, icon, is_paid. Else → insert new.
    """
    cursor.execute(
        "SELECT id, name FROM availability_types WHERE deleted_at IS NULL"
    )
    existing_by_name = {_normalize_name(r["name"]): r["id"] for r in cursor.fetchall()}

    to_insert = [t for t in types_deduped if _normalize_name(t["name"]) not in existing_by_name]
    to_update = [
        (t, existing_by_name[_normalize_name(t["name"])])
        for t in types_deduped
        if _normalize_name(t["name"]) in existing_by_name
    ]

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
                name = %s, type = %s, category = %s, description = %s,
                color = %s, icon = %s, is_paid = %s, last_modified_date = %s
            WHERE id = %s
            """,
            (
                t["name"], t["type"], t["category"], t["description"],
                t["color"], t["icon"], t["is_paid"], now, pk,
            ),
        )
        if cursor.rowcount:
            processed.append({
                "id": pk, "name": t["name"], "type": t["type"],
                "category": t["category"], "is_paid": t["is_paid"],
            })

    return processed


def seed_availability_types(connection, types):
    """
    Insert or update availability_types by name only.

    For each record: if a row with the same name (case-insensitive) exists,
    update its type, category, description, color, icon, is_paid; otherwise insert new.
    Duplicate names in input are deduplicated (last occurrence wins).
    """
    if not types:
        logger.warning("No availability types to insert")
        return False

    # First deduplicate by (name, type, category), then by name only so one record per name
    unique_types = deduplicate(types)
    types_by_name = _deduplicate_by_name(unique_types)
    if len(types_by_name) < len(unique_types):
        logger.info(
            "One row per name: reduced %s -> %s records",
            len(unique_types), len(types_by_name),
        )

    logger.info("Inserting/updating %s availability types (by name)...", len(types_by_name))

    cursor = connection.cursor()
    try:
        now = datetime.now()
        processed = _seed_by_name(cursor, types_by_name, now)
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
