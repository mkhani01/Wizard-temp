"""
Feasible Pairs Migration
Seeds feasible_pairs table from visit data CSV
Tracks caregiver-client visit frequencies
"""

import os
import csv
import logging
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

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
            cursor_factory=RealDictCursor
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


def load_existing_feasible_pairs(connection):
    """
    Load existing feasible pairs from database.
    Returns a dictionary: (caregiver_id, client_id) -> {id, frequency}
    
    Used for updating existing records instead of inserting duplicates.
    """
    cursor = connection.cursor()
    try:
        logger.info("Loading existing feasible pairs...")
        
        cursor.execute("""
            SELECT id, cgid, client_id, frequency 
            FROM feasible_pairs
        """)
        
        pairs = cursor.fetchall()
        lookup = {}
        
        for pair in pairs:
            key = (pair['cgid'], pair['client_id'])
            lookup[key] = {
                'id': pair['id'],
                'frequency': pair['frequency']
            }
        
        logger.info(f"✓ Loaded {len(lookup)} existing feasible pairs")
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


def extract_visit_frequencies_from_csv(csv_path, users_lookup, clients_lookup):
    """
    Extract visit frequencies from CSV file.
    
    For each row:
    1. Filter: Only process rows where both "Planned Service Type Description" 
       AND "Planned Service Requirement Type Description" equal "Personal Care"
    2. Parse "Planned Employee Name" to find caregiver (User) - format: "lastname, firstname"
    3. Parse "Service Location Name" to find client - format: "lastname, firstname"
    4. Count occurrences of each (caregiver, client) pair
    
    Returns a dictionary: (caregiver_id, client_id) -> frequency
    """
    frequencies = defaultdict(int)
    stats = {
        'total_rows': 0,
        'personal_care_rows': 0,
        'valid_rows': 0,
        'matched_pairs': 0,
        'unmatched_caregivers': set(),
        'unmatched_clients': set(),
        'skipped_non_personal_care': 0,
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
    
    logger.info(f"\nProcessing complete!")
    logger.info(f"  Total rows processed: {stats['total_rows']}")
    logger.info(f"  Rows skipped (non-Personal Care): {stats['skipped_non_personal_care']}")
    logger.info(f"  Personal Care rows: {stats['personal_care_rows']}")
    logger.info(f"  Valid visit records: {stats['valid_rows']}")
    logger.info(f"  Unique caregiver-client pairs: {stats['matched_pairs']}")
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
    
    return frequencies, stats


def seed_feasible_pairs(connection, frequencies, existing_pairs):
    """
    Insert or update feasible pairs based on frequency data.
    
    For new pairs: INSERT
    For existing pairs: UPDATE frequency (add to existing)
    """
    if not frequencies:
        logger.warning("No frequency data to insert.")
        return False
    
    cursor = connection.cursor()
    
    try:
        # Separate new pairs from updates
        new_pairs = []
        existing_updates = []
        
        for (caregiver_id, client_id), frequency in frequencies.items():
            pair_key = (caregiver_id, client_id)
            
            if pair_key in existing_pairs:
                # Update existing pair - add new frequency to existing
                existing_freq = existing_pairs[pair_key]['frequency']
                new_freq = existing_freq + frequency
                existing_updates.append((caregiver_id, client_id, new_freq))
            else:
                # Insert new pair
                new_pairs.append((caregiver_id, client_id, frequency))
        
        logger.info(f"\nSeeding feasible pairs to database...")
        logger.info(f"  New pairs to insert: {len(new_pairs)}")
        logger.info(f"  Existing pairs to update: {len(existing_updates)}")
        
        # Insert new pairs
        if new_pairs:
            insert_query = """
                INSERT INTO feasible_pairs (cgid, client_id, frequency, wait)
                VALUES %s
                RETURNING id, cgid, client_id, frequency
            """
            
            execute_values(
                cursor,
                insert_query,
                new_pairs,
                template="(%s, %s, %s, 0)"
            )
            
            inserted = cursor.fetchall()
            logger.info(f"✓ Inserted {len(inserted)} new feasible pairs")
            
            # Log sample
            if inserted:
                logger.info("Sample inserted pairs:")
                for pair in inserted[:5]:
                    logger.info(f"  - Caregiver ID: {pair['cgid']}, Client ID: {pair['client_id']}, Frequency: {pair['frequency']}")
        
        # Update existing pairs
        if existing_updates:
            update_count = 0
            for caregiver_id, client_id, frequency in existing_updates:
                update_query = """
                    UPDATE feasible_pairs 
                    SET frequency = %s 
                    WHERE cgid = %s AND client_id = %s
                """
                cursor.execute(update_query, (frequency, caregiver_id, client_id))
                update_count += cursor.rowcount
            
            logger.info(f"✓ Updated {update_count} existing feasible pairs")
        
        connection.commit()
        
        logger.info(f"\n✓ Successfully processed all feasible pairs")
        return True
        
    except Exception as e:
        connection.rollback()
        logger.error(f"✗ Failed to seed feasible pairs: {e}")
        raise
    finally:
        cursor.close()


def run(csv_path=None):
    """Main execution function. csv_path can be passed (e.g. from wizard) or from sys.argv, or default assets/visit_data.csv."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Feasible Pairs Migration                         ║
    ║         Seeding feasible_pairs from visit data CSV       ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Get database config
    config = get_db_config()
    
    # Validate config
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in environment variables")
        logger.error("Required: DB_NAME, DB_USER, DB_PASSWORD")
        return False
    
    # Get CSV path: explicit argument, then command line, then default
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
    
    # Check file size
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    logger.info(f"File size: {file_size_mb:.2f} MB")
    
    if file_size_mb > 100:
        logger.info("Large file detected - processing may take several minutes")
    
    # Connect and migrate
    connection = None
    try:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: DATABASE CONNECTION")
        logger.info("="*60)
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
        logger.info("STEP 4: LOAD EXISTING FEASIBLE PAIRS")
        logger.info("="*60)
        existing_pairs = load_existing_feasible_pairs(connection)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 5: EXTRACT VISIT FREQUENCIES FROM CSV")
        logger.info("="*60)
        frequencies, stats = extract_visit_frequencies_from_csv(csv_path, users_lookup, clients_lookup)
        
        if not frequencies:
            logger.warning("No valid visit frequencies found in CSV")
            return False
        
        logger.info("\n" + "="*60)
        logger.info("STEP 6: SEED FEASIBLE PAIRS TO DATABASE")
        logger.info("="*60)
        success = seed_feasible_pairs(connection, frequencies, existing_pairs)
        
        if success:
            print("\n" + "="*60)
            print("✓ FEASIBLE PAIRS MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            print(f"\nSummary:")
            print(f"  - Total CSV rows processed: {stats['total_rows']}")
            print(f"  - Rows skipped (non-Personal Care): {stats['skipped_non_personal_care']}")
            print(f"  - Personal Care rows: {stats['personal_care_rows']}")
            print(f"  - Valid visit records: {stats['valid_rows']}")
            print(f"  - Unique caregiver-client pairs: {stats['matched_pairs']}")
            print(f"  - Total visits recorded: {stats['total_visits']}")
            print(f"  - Unmatched caregivers: {len(stats['unmatched_caregivers'])}")
            print(f"  - Unmatched clients: {len(stats['unmatched_clients'])}")
            return True
        else:
            print("\n" + "="*60)
            print("✗ FEASIBLE PAIRS MIGRATION FAILED")
            print("="*60)
            return False
            
    except Exception as e:
        logger.error(f"Migration error: {e}", exc_info=True)
        return False
    finally:
        if connection:
            connection.close()
            logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)