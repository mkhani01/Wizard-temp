#!/usr/bin/env python3
"""
Pre-run sanity checks: run before starting the migration wizard or CLI.
Verifies environment, dependencies, and that migration modules are importable.
When run as a frozen exe (PyInstaller), skips dev-only checks (Python version, pip deps)
and uses the executable's directory as project root so no Python install is required.
"""

import sys
import os
from pathlib import Path

# When frozen (PyInstaller exe), project root = directory containing the exe (no Python needed).
# Otherwise, project root = parent of tests/.
if getattr(sys, "frozen", False):
    PROJECT_ROOT = Path(sys.executable).resolve().parent
    os.chdir(PROJECT_ROOT)
else:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    os.chdir(PROJECT_ROOT)


def check_python_version():
    """Require Python 3.8+."""
    if sys.version_info < (3, 8):
        print(f"  ✗ Python 3.8+ required; got {sys.version}")
        return False
    print(f"  ✓ Python {sys.version_info.major}.{sys.version_info.minor}")
    return True


def check_dependencies():
    """Import required packages."""
    required = [
        ("psycopg2", "psycopg2-binary"),
        ("dotenv", "python-dotenv"),
        ("openpyxl", "openpyxl"),
        ("pandas", "pandas"),
        ("requests", "requests"),
        ("geopy", "geopy"),
        ("PIL", "Pillow"),
        ("h3", "h3"),
    ]
    missing = []
    for mod, pkg in required:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"  ✗ Missing packages: {', '.join(missing)}. Run: pip install -r requirements.txt")
        return False
    print("  ✓ Dependencies OK")
    return True


def check_migration_imports():
    """Check that migration modules can be imported (no syntax/runtime errors)."""
    modules = [
        ("clientsMigration.main", "Clients migration"),
        ("usersMigration.main", "Users migration"),
        ("availabilityTypeMigration.main", "Availability types"),
        ("userAvailabilityMigration.main", "User availability"),
        ("clientAvailabilityMigration.main", "Client availability"),
        ("clientLocationsMigration.main", "Client locations"),
        ("userLocationsMigration.main", "User locations"),
        ("geocodeCalculation.main", "Geocode calculation"),
        ("areaMigration.main", "Area migration"),
        ("feasible_pairs_migration.feasible_pairs_migration", "Feasible pairs"),
        ("clientWindowsAnalyzer.main", "Client windows analyzer"),
        ("distance_migration.travel_distances_migration", "Travel distances"),
    ]
    failed = []
    for mod, name in modules:
        try:
            __import__(mod)
        except Exception as e:
            failed.append((name, str(e)))
    if failed:
        for name, err in failed:
            print(f"  ✗ {name}: {err}")
        return False
    print("  ✓ Migration modules importable")
    return True


def check_project_layout():
    """Check that expected dirs/files exist."""
    assets = PROJECT_ROOT / "assets"
    if not assets.is_dir():
        print("  ⚠ assets/ not found (will be created when you run the wizard)")
    else:
        print("  ✓ assets/ present")
    return True


def _is_frozen():
    """True when running as a PyInstaller one-file or one-dir exe (no Python install required)."""
    return getattr(sys, "frozen", False)


def run_all():
    """Run all pre-run checks. Returns True if all passed.
    When frozen (standalone exe), skips Python/pip checks so the exe is self-contained.
    """
    print("Pre-run checks (run before migration):\n")
    ok = True
    if not _is_frozen():
        ok &= check_python_version()
        ok &= check_dependencies()
    else:
        # Bundled exe: runtime and deps are already inside the exe
        print("  ✓ Standalone executable (checks simplified)")
    ok &= check_migration_imports()
    ok &= check_project_layout()
    print("")
    return ok


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
