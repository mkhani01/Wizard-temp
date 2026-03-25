"""
Feasible Pairs Migration
Seeds feasible_pairs table from visit data CSV
Tracks caregiver-client visit frequencies
"""

import os
import csv
import logging
import math
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import OperationalError, InterfaceError

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('feasible_pairs_migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_db_config():
    """Get database configuration from environment variables"""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }


def connect_to_database(config):
    """Connect to PostgreSQL database"""
    try:
        logger.info("Connecting to PostgreSQL...")
        logger.info(f"  Host: {config['host']}")
        logger.info(f"  Port: {config['port']}")
        logger.info(f"  Database: {config['database']}")
        logger.info(f"  User: {config['user']}")
        
        connection = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            cursor_factory=RealDictCursor,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        connection.autocommit = False
        logger.info("✓ Connected to database")
        return connection
    except Exception as e:
        logger.error(f"✗ Failed to connect: {e}")
        raise


def safe_strip(value):
    """Safely strip a value that might be None. Removes Excel formula wrapper =\"...\" if present."""
    if value is None:
        return ''
    s = str(value).strip()
    if s.startswith('="') and s.endswith('"'):
        s = s[2:-1].strip()
    elif s.startswith('=') and len(s) > 1:
        s = s[1:].strip()
    return s


def parse_full_name(full_name):
    """
    Parse a full name into first name and last name.
    Supports:
      - "Firstname Lastname" (last word = last name)
      - "Lastname, Firstname" (e.g. VisitExport / Caremark style)
    
    Examples:
        "Bernard Prendergast" -> ("Bernard", "Prendergast")
        "Geiser, Tatijana" -> ("Tatijana", "Geiser")
    """
    if not full_name:
        return (None, None)
    
    full_name = safe_strip(full_name)
    if not full_name:
        return (None, None)
    
    # "Lastname, Firstname" format (VisitExport / Caremark)
    if ',' in full_name:
        parts = [p.strip() for p in full_name.split(',', 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            return (parts[1], parts[0])  # (firstname, lastname)
        if len(parts) == 1 and parts[0]:
            return (parts[0], None)
        return (None, None)
    
    parts = full_name.split()
    if len(parts) == 0:
        return (None, None)
    elif len(parts) == 1:
        return (parts[0], None)
    else:
        lastname = parts[-1]
        firstname = ' '.join(parts[:-1])
        return (firstname, lastname)


def load_users_lookup(connection):
    """
    Load all users (caregivers) from database into a lookup dictionary.
    Key: (name, lastname) lowercase -> Value: user_id
    
    This allows us to quickly find user IDs by name combination.
    Also stores multiple variations for flexible matching.
    """
    cursor = connection.cursor()
    try:
        logger.info("Loading caregivers from database...")
        
        cursor.execute("""
            SELECT id, name, lastname, email 
            FROM "user" 
            WHERE deleted_at IS NULL AND is_caregiver = true
        """)
        
        users = cursor.fetchall()
        lookup = {}
        
        for user in users:
            name = safe_strip(user['name'])
            lastname = safe_strip(user['lastname'])
            
            # Primary key: exact match (lowercase)
            key = (name.lower(), lastname.lower())
            lookup[key] = user['id']
            
            # Also try with preferred name variations
            if user.get('preferred_name'):
                pref_key = (safe_strip(user['preferred_name']).lower(), lastname.lower())
                if pref_key not in lookup:
                    lookup[pref_key] = user['id']
        
        logger.info(f"✓ Loaded {len(users)} caregivers ({len(lookup)} lookup keys)")
        
        # Log sample for verification
        if users:
            logger.info("Sample caregivers loaded:")
            for user in users[:5]:
                logger.info(f"  - {user['name']} {user['lastname']} (ID: {user['id']})")
        
        return lookup
        
    finally:
        cursor.close()


def load_clients_lookup(connection):
    """
    Load all clients from database into a lookup dictionary.
    Key: (name, lastname) lowercase -> Value: client_id
    
    This allows us to quickly find client IDs by name combination.
    """
    cursor = connection.cursor()
    try:
        logger.info("Loading clients from database...")
        
        cursor.execute("""
            SELECT id, name, lastname 
            FROM client 
            WHERE deleted_at IS NULL
        """)
        
        clients = cursor.fetchall()
        lookup = {}
        
        for client in clients:
            name = safe_strip(client['name'])
            lastname = safe_strip(client['lastname'])
            
            # Primary key: exact match (lowercase)
            key = (name.lower(), lastname.lower())
            lookup[key] = client['id']
            
            # Also try with preferred name if available
            if client.get('preferred_name'):
                pref_key = (safe_strip(client['preferred_name']).lower(), lastname.lower())
                if pref_key not in lookup:
                    lookup[pref_key] = client['id']
        
        logger.info(f"✓ Loaded {len(clients)} clients ({len(lookup)} lookup keys)")
        
        # Log sample for verification
        if clients:
            logger.info("Sample clients loaded:")
            for client in clients[:5]:
                logger.info(f"  - {client['name']} {client['lastname']} (ID: {client['id']})")
        
        return lookup
        
    finally:
        cursor.close()


def is_personal_care_row(row):
    """
    Check if the row represents a Personal Care service.
    Both 'Planned Service Type Description' AND 'Planned Service Requirement Type Description'
    must equal "Personal Care".
    """
    service_type = safe_strip(row.get('Planned Service Type Description', ''))
    requirement_type = safe_strip(row.get('Planned Service Requirement Type Description', ''))
    
    return service_type == 'Personal Care' and requirement_type == 'Personal Care'


def parse_visit_datetime(row):
    """
    Parse visit start datetime from CSV row.
    Uses day-first parsing fallback to align with feasibility script behavior.
    """
    candidate_columns = [
        'Service Requirement Start Date And Time',
        'Planned Service Requirement Start Date And Time',
        'Actual Service Start Date And Time',
    ]
    date_formats = [
        '%d/%m/%Y %H:%M',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y %I:%M %p',
        '%d/%m/%Y %I:%M:%S %p',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
    ]

    for column in candidate_columns:
        raw_value = safe_strip(row.get(column, ''))
        if not raw_value:
            continue

        normalized = raw_value.replace('T', ' ')

        for fmt in date_formats:
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue

        iso_value = raw_value.replace('Z', '+00:00')
        try:
            parsed = datetime.fromisoformat(iso_value)
            return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except ValueError:
            continue

    return None


def identify_carer_status(overall_pct, days_since_last_visit):
    """Apply status mapping from feasibility scripts."""
    if days_since_last_visit > 50:
        return "Former / Relief"
    if overall_pct >= 40:
        return "Current Primary"
    return "Support / Relief"


def calculate_pair_weights(frequencies, pair_last_visit, customer_totals, dataset_end):
    """
    Calculate normalized weights (0-1) per caregiver-client pair.
    Mirrors set_roster.py + feaibility_percentage.py logic.
    """
    if not dataset_end:
        return {}

    window_days = 16 * 7  # 112
    status_factors = {
        'Current Primary': 1.0,
        'Support / Relief': 0.5,
        'Former / Relief': 0.2,
    }

    raw_weights = {}
    max_raw_by_client = defaultdict(float)

    for pair_key, total_pair_visits in frequencies.items():
        _, client_id = pair_key
        total_cust_visits = customer_totals.get(client_id, 0)
        if total_cust_visits <= 0:
            raw_weights[pair_key] = 0.0
            continue

        last_visit = pair_last_visit.get(pair_key)
        days_since_last_visit = 999
        if last_visit:
            days_since_last_visit = max((dataset_end - last_visit).days, 0)

        overall_pct = round((total_pair_visits / total_cust_visits) * 100, 1)
        carer_status = identify_carer_status(overall_pct, days_since_last_visit)

        consistency = overall_pct / 100.0
        calls_per_day = total_cust_visits / float(window_days)
        freq_factor = 1 + math.log1p(calls_per_day)
        recency_decay = math.exp(-days_since_last_visit / 21.0)
        status_factor = status_factors.get(carer_status, 0.3)

        raw_weight = consistency * freq_factor * recency_decay * status_factor
        raw_weights[pair_key] = raw_weight

        if raw_weight > max_raw_by_client[client_id]:
            max_raw_by_client[client_id] = raw_weight

    weights = {}
    for pair_key, raw_weight in raw_weights.items():
        _, client_id = pair_key
        client_max = max_raw_by_client.get(client_id, 0.0)
        normalized = 0.0 if client_max <= 0 else raw_weight / client_max
        weights[pair_key] = round(normalized, 4)

    return weights


def extract_visit_frequencies_from_csv(csv_path, users_lookup, clients_lookup):
    """
    Extract visit frequencies from CSV file.
    
    For each row:
    1. Filter: Only process rows where both "Planned Service Type Description" 
       AND "Planned Service Requirement Type Description" equal "Personal Care"
    2. Parse "Planned Employee Name" to find caregiver (User) - format: "lastname, firstname"
    3. Parse "Service Location Name" to find client - format: "lastname, firstname"
    4. Parse visit date/time to compute recency-based weight
    5. Count occurrences of each (caregiver, client) pair and compute weight

    Returns:
      - frequencies: (caregiver_id, client_id) -> frequency
      - weights: (caregiver_id, client_id) -> weight (0..1, rounded to 4 decimals)
      - stats
    """
    frequencies = defaultdict(int)
    pair_last_visit = {}
    customer_totals = defaultdict(int)
    dataset_end = None

    stats = {
        'total_rows': 0,
        'personal_care_rows': 0,
        'valid_rows': 0,
        'matched_pairs': 0,
        'unmatched_caregivers': set(),
        'unmatched_clients': set(),
        'skipped_non_personal_care': 0,
        'skipped_invalid_datetime': 0,
    }
    
    logger.info(f"Reading CSV: {csv_path}")
    logger.info("Processing visit data (this may take a while for large files)...")
    logger.info("Filter: Only 'Personal Care' service types will be processed")
    
    # Process in batches to handle large files efficiently
    batch_size = 10000
    processed = 0
    
    def get_employee_name(row):
        """Get employee name from 'Planned Employee Name' column (format: lastname, firstname)"""
        v = row.get('Planned Employee Name')
        return safe_strip(v) if v else ''
    
    def get_service_location_name(row):
        """Get client name from 'Service Location Name' column (format: lastname, firstname)"""
        v = row.get('Service Location Name')
        return safe_strip(v) if v else ''

    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        row_num = 0

        for row in reader:
            row_num += 1
            stats['total_rows'] += 1
            processed += 1

            # Filter: Only process Personal Care rows
            if not is_personal_care_row(row):
                stats['skipped_non_personal_care'] += 1
                logger.warning(
                    "Row %d: SKIPPED - not Personal Care | Employee=%r, Location=%r, ServiceType=%r",
                    row_num, row.get('Planned Employee Name'), row.get('Service Location Name'), row.get('Planned Service Type Description')
                )
                continue

            stats['personal_care_rows'] += 1

            # Get employee name (caregiver) from "Planned Employee Name"
            employee_name = get_employee_name(row)
            # Get client name from "Service Location Name"
            service_location_name = get_service_location_name(row)

            if not employee_name or not service_location_name:
                logger.warning(
                    "Row %d: SKIPPED - missing employee or client name | Employee=%r, Location=%r",
                    row_num, employee_name, service_location_name
                )
                continue

            visit_start = parse_visit_datetime(row)
            if not visit_start:
                stats['skipped_invalid_datetime'] += 1
                logger.warning(
                    "Row %d: SKIPPED - invalid visit datetime | Employee=%r, Location=%r",
                    row_num, employee_name, service_location_name
                )
                continue

            # Parse employee name into first name and last name
            employee_first, employee_last = parse_full_name(employee_name)
            # Parse service location name into first name and last name
            client_first, client_last = parse_full_name(service_location_name)

            if not employee_first or not employee_last:
                stats['unmatched_caregivers'].add(employee_name)
                logger.warning(
                    "Row %d: SKIPPED - could not parse caregiver name | Employee=%r, Location=%r",
                    row_num, employee_name, service_location_name
                )
                continue

            if not client_first or not client_last:
                stats['unmatched_clients'].add(service_location_name)
                logger.warning(
                    "Row %d: SKIPPED - could not parse client name | Employee=%r, Location=%r",
                    row_num, employee_name, service_location_name
                )
                continue

            # Look up caregiver in users table
            caregiver_key = (employee_first.lower(), employee_last.lower())
            caregiver_id = users_lookup.get(caregiver_key)
            if not caregiver_id:
                first_part = employee_first.split()[0] if employee_first else ''
                alt_key = (first_part.lower(), employee_last.lower())
                caregiver_id = users_lookup.get(alt_key)

            # Look up client in clients table
            client_key = (client_first.lower(), client_last.lower())
            client_id = clients_lookup.get(client_key)
            if not client_id:
                first_part = client_first.split()[0] if client_first else ''
                alt_key = (first_part.lower(), client_last.lower())
                client_id = clients_lookup.get(alt_key)

            # Record the pair if both found
            if caregiver_id and client_id:
                pair_key = (caregiver_id, client_id)
                frequencies[pair_key] += 1
                customer_totals[client_id] += 1
                if pair_key not in pair_last_visit or visit_start > pair_last_visit[pair_key]:
                    pair_last_visit[pair_key] = visit_start
                if dataset_end is None or visit_start > dataset_end:
                    dataset_end = visit_start
                stats['valid_rows'] += 1
                logger.info(
                    "Row %d: ADDED | caregiver=%r (id=%s), client=%r (id=%s), frequency=%d",
                    row_num, employee_name, caregiver_id, service_location_name, client_id, frequencies[pair_key]
                )
            else:
                if not caregiver_id:
                    stats['unmatched_caregivers'].add(employee_name)
                if not client_id:
                    stats['unmatched_clients'].add(service_location_name)
                logger.warning(
                    "Row %d: SKIPPED - unmatched | caregiver=%r (found=%s), client=%r (found=%s)",
                    row_num, employee_name, caregiver_id is not None, service_location_name, client_id is not None
                )

            # Log progress every batch
            if processed % batch_size == 0:
                logger.info("  Processed %d rows...", processed)
    
    # Calculate total matched pairs (sum of all frequencies)
    stats['matched_pairs'] = len(frequencies)
    stats['total_visits'] = sum(frequencies.values())
    weights = calculate_pair_weights(frequencies, pair_last_visit, customer_totals, dataset_end)
    stats['weighted_pairs'] = len(weights)
    
    logger.info(f"\nProcessing complete!")
    logger.info(f"  Total rows processed: {stats['total_rows']}")
    logger.info(f"  Rows skipped (non-Personal Care): {stats['skipped_non_personal_care']}")
    logger.info(f"  Rows skipped (invalid datetime): {stats['skipped_invalid_datetime']}")
    logger.info(f"  Personal Care rows: {stats['personal_care_rows']}")
    logger.info(f"  Valid visit records: {stats['valid_rows']}")
    logger.info(f"  Unique caregiver-client pairs: {stats['matched_pairs']}")
    logger.info(f"  Pairs with calculated weight: {stats['weighted_pairs']}")
    logger.info(f"  Total visits recorded: {stats['total_visits']}")
    logger.info(f"  Unique unmatched caregivers: {len(stats['unmatched_caregivers'])}")
    logger.info(f"  Unique unmatched clients: {len(stats['unmatched_clients'])}")
    
    if stats['unmatched_caregivers']:
        logger.info(f"\nSample unmatched caregivers (up to 10):")
        for name in list(stats['unmatched_caregivers'])[:10]:
            logger.info(f"  - {name}")
    
    if stats['unmatched_clients']:
        logger.info(f"\nSample unmatched clients (up to 10):")
        for name in list(stats['unmatched_clients'])[:10]:
            logger.info(f"  - {name}")
    
    return frequencies, weights, stats


def truncate_feasible_pairs(connection):
    """Truncate feasible_pairs so CSV is the complete source of truth."""
    cursor = connection.cursor()
    try:
        logger.info("Truncating feasible_pairs table...")
        cursor.execute("TRUNCATE TABLE feasible_pairs RESTART IDENTITY")
        connection.commit()
        logger.info("✓ Truncated feasible_pairs")
    except Exception as e:
        connection.rollback()
        logger.error(f"✗ Failed to truncate feasible_pairs: {e}")
        raise
    finally:
        cursor.close()


def seed_feasible_pairs(connection, frequencies, weights):
    """
    Insert feasible pairs after table truncate.
    Each row includes frequency and normalized weight.
    """
    if not frequencies:
        logger.warning("No frequency data to insert.")
        return False

    cursor = connection.cursor()
    try:
        pairs_data = [
            (cgid, cid, freq, float(weights.get((cgid, cid), 0.0)))
            for (cgid, cid), freq in frequencies.items()
        ]
        logger.info("\nSeeding feasible pairs to database (fresh insert after truncate)...")
        logger.info(f"  Pairs to insert: {len(pairs_data)}")

        insert_query = """
            INSERT INTO feasible_pairs (cgid, client_id, frequency, weight)
            VALUES %s
        """
        execute_values(cursor, insert_query, pairs_data, template="(%s, %s, %s, %s)")
        connection.commit()
        logger.info(f"✓ Successfully inserted {len(pairs_data)} feasible pairs")
        return True

    except (OperationalError, InterfaceError) as e:
        connection.rollback()
        if ConnectionLostError:
            raise ConnectionLostError("feasible_pairs", {}) from e
        raise
    except Exception as e:
        connection.rollback()
        logger.error(f"✗ Failed to seed feasible pairs: {e}")
        raise
    finally:
        cursor.close()


def run(csv_path=None, connection_manager=None, state=None):
    """Main execution function. connection_manager and state used from wizard for resume support."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Feasible Pairs Migration                         ║
    ║         Seeding feasible_pairs from visit data CSV       ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    if state and state.is_completed("feasible_pairs"):
        logger.info("Feasible pairs migration already completed (resume).")
        return True
    config = get_db_config()
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in environment variables")
        logger.error("Required: DB_NAME, DB_USER, DB_PASSWORD")
        return False
    if csv_path is not None:
        csv_path = Path(csv_path)
    elif len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        from migration_support import get_assets_dir
        csv_path = get_assets_dir() / 'visit_data.csv'
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        logger.info("Usage: python feasible_pairs_migration.py <path_to_csv>")
        return False
    logger.info(f"CSV file: {csv_path}")
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    logger.info(f"File size: {file_size_mb:.2f} MB")
    if file_size_mb > 100:
        logger.info("Large file detected - processing may take several minutes")
    connection = None
    try:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: DATABASE CONNECTION")
        logger.info("="*60)
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
            connection = connect_to_database(config)
        logger.info("\n" + "="*60)
        logger.info("STEP 2: LOAD CAREGIVERS (USERS)")
        logger.info("="*60)
        users_lookup = load_users_lookup(connection)
        logger.info("\n" + "="*60)
        logger.info("STEP 3: LOAD CLIENTS")
        logger.info("="*60)
        clients_lookup = load_clients_lookup(connection)
        logger.info("\n" + "="*60)
        logger.info("STEP 4: EXTRACT VISIT FREQUENCIES AND WEIGHTS FROM CSV")
        logger.info("="*60)
        frequencies, weights, stats = extract_visit_frequencies_from_csv(csv_path, users_lookup, clients_lookup)
        if not frequencies:
            logger.warning("No valid visit frequencies found in CSV")
            return False
        logger.info("\n" + "="*60)
        logger.info("STEP 5: TRUNCATE FEASIBLE PAIRS TABLE")
        logger.info("="*60)
        truncate_feasible_pairs(connection)
        logger.info("\n" + "="*60)
        logger.info("STEP 6: SEED FEASIBLE PAIRS TO DATABASE")
        logger.info("="*60)
        success = seed_feasible_pairs(connection, frequencies, weights)
        if success:
            if state:
                state.update("feasible_pairs", status="completed")
                state.clear_step("feasible_pairs")
            print("\n" + "="*60)
            print("✓ FEASIBLE PAIRS MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            print(f"\nSummary:")
            print(f"  - Total CSV rows processed: {stats['total_rows']}")
            print(f"  - Rows skipped (non-Personal Care): {stats['skipped_non_personal_care']}")
            print(f"  - Rows skipped (invalid datetime): {stats['skipped_invalid_datetime']}")
            print(f"  - Personal Care rows: {stats['personal_care_rows']}")
            print(f"  - Valid visit records: {stats['valid_rows']}")
            print(f"  - Unique caregiver-client pairs: {stats['matched_pairs']}")
            print(f"  - Pairs with calculated weight: {stats['weighted_pairs']}")
            print(f"  - Total visits recorded: {stats['total_visits']}")
            print(f"  - Unmatched caregivers: {len(stats['unmatched_caregivers'])}")
            print(f"  - Unmatched clients: {len(stats['unmatched_clients'])}")
            return True
        print("\n" + "="*60)
        print("✗ FEASIBLE PAIRS MIGRATION FAILED")
        print("="*60)
        return False
    except (OperationalError, InterfaceError) as e:
        if ConnectionLostError:
            raise ConnectionLostError("feasible_pairs", {}) from e
        raise
    except Exception as e:
        logger.error(f"Migration error: {e}", exc_info=True)
        return False
    finally:
        if connection and not connection_manager:
            connection.close()
            logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
