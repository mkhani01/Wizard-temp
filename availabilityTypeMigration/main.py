"""
Availability Types Migration – seeds availability_types table from CSV.

Runnable from:
  - Migration Wizard (reads from assets/availabilitytypes/*.csv after wizard copies files)
  - CLI: python main.py availability-types  [path_to_csv_or_folder]
        or from project root: python -m availabilityTypeMigration [path]

CSV format (e.g. availabilityTypes.csv):
  Name, Type, Description, Is Paid, Color, Category
Maps to NestJS AvailabilityType: name, type, category, description, color, icon, is_paid.
"""

import logging
import sys
from pathlib import Path

from . import config as db_config
from . import csv_parser
from . import db_seed
from migration_support import get_project_root, get_assets_dir

logger = logging.getLogger(__name__)

# Default assets path (wizard copies files here; uses exe dir when frozen)
PROJECT_ROOT = get_project_root()
DEFAULT_CSV_DIR = get_assets_dir() / "availabilitytypes"


def _setup_logging(log_to_file=True):
    """Configure logging. When run from wizard, root logger may already be configured."""
    root = logging.getLogger()
    if root.handlers:
        return
    handlers = [logging.StreamHandler()]
    if log_to_file:
        log_file = PROJECT_ROOT / "availability_types_migration.log"
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def _collect_csv_paths(path_arg=None):
    """
    Return list of CSV paths to process.
    - If path_arg is a file: [path_arg]
    - If path_arg is a folder: all *.csv in that folder
    - If path_arg is None: use DEFAULT_CSV_DIR (assets/availabilitytypes)
    """
    if path_arg is not None:
        p = Path(path_arg)
        if not p.exists():
            logger.error("Path does not exist: %s", p)
            return []
        if p.is_file():
            return [p] if p.suffix.lower() == ".csv" else []
        return sorted(p.glob("*.csv"))
    # Default: folder used by wizard
    if not DEFAULT_CSV_DIR.exists():
        return []
    return sorted(DEFAULT_CSV_DIR.glob("*.csv"))


try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None


def run(csv_path_or_folder=None, connection_manager=None, state=None):
    """
    Main entry point. Run migration from CSV to availability_types table.
    connection_manager and state are used when run from wizard for resume support.
    """
    _setup_logging(log_to_file=True)
    if state and state.is_completed("availability_types"):
        logger.info("Availability types migration already completed (resume).")
        return True
    print(
        """
    ╔══════════════════════════════════════════════════════════╗
    ║         Availability Types Migration                     ║
    ║         Seeding availability_types from CSV              ║
    ╚══════════════════════════════════════════════════════════╝
    """
    )
    config = db_config.get_db_config()
    if not all([config["database"], config["user"], config["password"]]):
        logger.error("Missing database configuration. Set DB_NAME, DB_USER, DB_PASSWORD (e.g. in .env)")
        return False
    csv_paths = _collect_csv_paths(csv_path_or_folder)
    if not csv_paths:
        if csv_path_or_folder is not None:
            logger.error("No CSV files found at: %s", csv_path_or_folder)
        else:
            logger.error(
                "No CSV files found in %s. Create the folder and add CSV(s), or run from the wizard after selecting the availability types folder.",
                DEFAULT_CSV_DIR,
            )
        return False
    all_types = []
    for csv_path in csv_paths:
        logger.info("\n" + "=" * 60)
        logger.info("PROCESSING: %s", csv_path.name)
        logger.info("=" * 60)
        try:
            types = csv_parser.extract_from_csv(csv_path)
            all_types.extend(types)
        except Exception as e:
            logger.exception("Failed to parse %s: %s", csv_path, e)
            return False
    if not all_types:
        logger.warning("No availability types extracted from any CSV")
        return False
    connection = None
    try:
        logger.info("\n" + "=" * 60)
        logger.info("STEP: DATABASE CONNECTION")
        logger.info("=" * 60)
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
            connection = db_config.connect_to_database(config)
        logger.info("\n" + "=" * 60)
        logger.info("STEP: SEED AVAILABILITY TYPES")
        logger.info("=" * 60)
        try:
            success = db_seed.seed_availability_types(connection, all_types)
        except Exception as e:
            import psycopg2
            if isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError)) and ConnectionLostError:
                raise ConnectionLostError("availability_types", {}) from e
            raise
        if success:
            if state:
                state.clear_step("availability_types")
            print("\n" + "=" * 60)
            print("✓ AVAILABILITY TYPES MIGRATION COMPLETED SUCCESSFULLY")
            print("=" * 60)
            return True
        print("\n" + "=" * 60)
        print("✗ AVAILABILITY TYPES MIGRATION FAILED")
        print("=" * 60)
        return False
    except Exception as e:
        logger.exception("Migration error: %s", e)
        return False
    finally:
        if connection and not connection_manager:
            connection.close()
            logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    # CLI: optional path as first argument
    path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    success = run(csv_path_or_folder=path_arg)
    sys.exit(0 if success else 1)
