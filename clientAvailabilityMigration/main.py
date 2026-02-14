"""
Client Availability Migration - entry point for wizard and CLI.
Uses migrate_client_availability logic without changing it.
Reads Excel (Client Hours with Service Type), writes SQL/TS/JSON/log to output_dir.
"""

import os
import logging
from pathlib import Path

# Project root (parent of clientAvailabilityMigration)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ASSETS = PROJECT_ROOT / "assets"

# Default paths when run from wizard (files copied to assets)
DEFAULT_INPUT_FILENAME = "ClientHoursWithServiceType.xlsx"


def run(file_path=None, output_dir=None):
    """
    Run the Client Hours -> ClientAvailability migration.
    Uses migrate_client_availability logic; does not change that module.

    Args:
        file_path: Path to the Excel file (sheet 'Data', Client Hours with Service Type).
                   If None, uses ASSETS/ClientHoursWithServiceType.xlsx or env CLIENT_HOURS_EXCEL_PATH.
        output_dir: Directory for SQL, TS, JSON, and log outputs. If None, uses PROJECT_ROOT.

    Returns:
        True on success, False on failure.
    """
    from clientAvailabilityMigration.migrate_client_availability import (
        process_excel_data,
        generate_sql_migration,
        generate_typescript_migration,
        generate_json_output,
        generate_log_report,
    )

    input_path = file_path
    if input_path is None:
        input_path = os.getenv("CLIENT_HOURS_EXCEL_PATH") or str(ASSETS / DEFAULT_INPUT_FILENAME)
    input_path = Path(input_path)

    out_dir = output_dir
    if out_dir is None:
        out_dir = os.getenv("CLIENT_AVAILABILITY_OUTPUT_DIR") or str(PROJECT_ROOT)
    out_path = Path(out_dir)

    if not input_path.exists():
        logging.error("Client Hours Excel file not found: %s", input_path)
        return False

    try:
        data = process_excel_data(str(input_path))
    except Exception as e:
        logging.exception("Failed to process Excel: %s", e)
        return False

    out_path.mkdir(parents=True, exist_ok=True)

    try:
        sql_content = generate_sql_migration(data)
        (out_path / "client_availability_migration.sql").write_text(sql_content, encoding="utf-8")
        ts_content = generate_typescript_migration(data)
        (out_path / "client_availability_migration.ts").write_text(ts_content, encoding="utf-8")
        json_content = generate_json_output(data)
        (out_path / "client_availability_data.json").write_text(json_content, encoding="utf-8")
        log_content = generate_log_report(data)
        (out_path / "migration_log.txt").write_text(log_content, encoding="utf-8")
    except Exception as e:
        logging.exception("Failed to write outputs: %s", e)
        return False

    logging.info("Client availability migration outputs written to %s", out_path)
    return True
