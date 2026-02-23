import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Ensure project/bundle root is on path (script dir when dev; PyInstaller bundle when single-file exe)
if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
    _PROJECT_ROOT = Path(sys._MEIPASS).resolve()
    _ASSETS_ROOT = Path(sys.executable).resolve().parent
else:
    _PROJECT_ROOT = Path(__file__).resolve().parent
    _ASSETS_ROOT = _PROJECT_ROOT
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
os.environ["AOS_MIGRATION_PROJECT_ROOT"] = str(_ASSETS_ROOT)
# When frozen, use exe dir for .env and cwd (logs, cache, assets) so it works regardless of launch cwd
if getattr(sys, "frozen", False):
    os.chdir(_ASSETS_ROOT)
    load_dotenv(_ASSETS_ROOT / ".env")
else:
    load_dotenv()


def print_usage():
    """Print usage information"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Caremark Migration Tool                          ║
    ╚══════════════════════════════════════════════════════════╝
    
    Usage:
        python3 main.py <command>
    
    Commands:
        areas                Migrate areas/teams to users_group table
        users                Migrate users
        clients              Migrate clients
        availabilities [excel_file] [output_dir]   Migrate client availabilities (Client Hours with Service Type). Optional: path to XLSX and output directory.
        user-availabilities  Migrate user availabilities
        availability-types [path]   Migrate availability types from CSV (path = file or folder; default: assets/availabilitytypes)
        geocode-calculation  Run geocode calculation
        userlocations        Update user lat/lng from JSON backup
        clientlocations      Update client lat/lng from JSON backup
        travel-distances     Compute user<->client distances via OSRM, upsert travel_distances, then verify
        feasible-pairs [path]   Seed feasible_pairs from visit data CSV (default: assets/visit_data.csv)
        client-windows [path]   Update client_availabilities start/end/minDuration from visit CSV (default: assets/client_windows_data.csv)
        test                 Run pre-run checks and optional distance test (run before migrating)
        all                  Run all migrations (TODO)
    
    Examples:
        python3 main.py areas
        python3 main.py userlocations
        python3 main.py clientlocations
    """)

def run_travel_distances_migration():
    """Run travel distances migration"""
    from distance_migration.travel_distances_migration import run as run_travel_distances_migration
    run_travel_distances_migration()

def run_areas_migration():
    """Run area migration"""
    # Import and run area migration
    from areaMigration.main import run as run_area_migration
    run_area_migration()

def run_users_migration():
    """Run users migration"""
    # Import and run users migration
    from usersMigration.main import run as run_users_migration
    run_users_migration()

def run_clients_migration():
    """Run clients migration"""
    from clientsMigration.main import run as run_clients_migration
    run_clients_migration()

def run_availability_migration(excel_file=None, output_dir=None):
    """Run client availability migration (Client Hours with Service Type -> ClientAvailability). Optional excel_file and output_dir."""
    from clientAvailabilityMigration.main import run as run_client_availability
    ok = run_client_availability(file_path=excel_file, output_dir=output_dir)
    if not ok:
        sys.exit(1)

def run_user_availability_migration():
    """Run user availability migration"""
    from userAvailabilityMigration.main import run as run_user_availability_migration
    run_user_availability_migration()

def run_availability_types_migration(csv_path_or_folder=None):
    """Run availability types migration (CSV → availability_types table). Optional: path to CSV file or folder."""
    from availabilityTypeMigration.main import run as run_availability_types_migration
    return run_availability_types_migration(csv_path_or_folder=csv_path_or_folder)

def run_geocode_calculation():
    """Run geocode calculation migration"""
    from geocodeCalculation.main import run as run_geocode_calculation
    run_geocode_calculation()

def run_user_locations_migration():
    """Run user locations migration"""
    from userLocationsMigration.main import run as run_user_locations_migration
    run_user_locations_migration()

def run_client_locations_migration():
    """Run client locations migration"""
    from clientLocationsMigration.main import run as run_client_locations_migration
    run_client_locations_migration()

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'areas':
        run_areas_migration()
    elif command == 'users':
        run_users_migration()
    elif command == 'clients':
        run_clients_migration()
    elif command == 'availabilities':
        excel_file = sys.argv[2] if len(sys.argv) > 2 else None
        output_dir = sys.argv[3] if len(sys.argv) > 3 else None
        run_availability_migration(excel_file=excel_file, output_dir=output_dir)
    elif command == 'user-availabilities':
        run_user_availability_migration()
    elif command == 'availability-types':
        path_arg = sys.argv[2] if len(sys.argv) > 2 else None
        success = run_availability_types_migration(path_arg)
        sys.exit(0 if success else 1)
    elif command == 'geocode-calculation':  
        run_geocode_calculation()
    elif command == 'userlocations':
        run_user_locations_migration()
    elif command == 'clientlocations':
        run_client_locations_migration()
    elif command == 'travel-distances':
        run_travel_distances_migration()
    elif command == 'feasible-pairs':
        csv_path = sys.argv[2] if len(sys.argv) > 2 else None
        from feasible_pairs_migration.feasible_pairs_migration import run as run_feasible_pairs
        success = run_feasible_pairs(csv_path=csv_path)
        sys.exit(0 if success else 1)
    elif command == 'client-windows':
        csv_path = sys.argv[2] if len(sys.argv) > 2 else None
        from clientWindowsAnalyzer.main import run as run_client_windows
        success = run_client_windows(csv_path=csv_path)
        sys.exit(0 if success else 1)
    elif command == 'test':
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from tests.run_tests import main as run_tests_main
        sys.exit(run_tests_main())
    elif command == 'all':
        print("Full migration not yet implemented")
        sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        print_usage()
        sys.exit(1)

if __name__ == "__main__":
    main()