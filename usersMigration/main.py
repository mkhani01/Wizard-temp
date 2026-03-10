"""
Users Migration
Seeds user table from CSV and links to groups
"""

import os
import re
import csv
import logging
from pathlib import Path
from datetime import datetime, date

from migration_support import get_assets_dir
from encoding_utils import fix_utf8_mojibake
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import OperationalError, InterfaceError

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

USER_BATCH_SIZE = 500
KEEPALIVES = dict(keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
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
            **KEEPALIVES,
        )
        connection.autocommit = False
        logger.info("✓ Connected to database")
        return connection
    except Exception as e:
        logger.error(f"✗ Failed to connect: {e}")
        raise


def get_lookup_tables(connection):
    """Get all lookup table data for foreign key mapping"""
    cursor = connection.cursor()
    try:
        lookups = {}
        
        # Get titles
        cursor.execute("SELECT id, name FROM title")
        lookups['titles'] = {row['name']: row['id'] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(lookups['titles'])} titles")
        
        # Get nationalities
        cursor.execute("SELECT id, name FROM nationality")
        lookups['nationalities'] = {row['name']: row['id'] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(lookups['nationalities'])} nationalities")
        
        # Get religions
        cursor.execute("SELECT id, name FROM religion")
        lookups['religions'] = {row['name']: row['id'] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(lookups['religions'])} religions")
        
        # Get origins (ethnic origins)
        cursor.execute("SELECT id, name FROM origin")
        lookups['origins'] = {row['name']: row['id'] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(lookups['origins'])} origins")
        
        # Get groups (users_group) - for many-to-many relationship
        cursor.execute("SELECT id, name FROM users_group")
        lookups['groups'] = {row['name']: row['id'] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(lookups['groups'])} groups")
        
        return lookups
        
    finally:
        cursor.close()


def parse_date(date_str):
    """Parse date from CSV format DD/MM/YYYY with optional time (HH:MM:SS or HH:MM)"""
    if not date_str or date_str.strip() == '':
        return None
    s = date_str.strip()
    for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    logger.warning(f"Could not parse date: {date_str}")
    return None


def map_gender(csv_gender):
    """Map CSV gender to entity enum"""
    gender_map = {
        'Male': 'Male',
        'Female': 'Female',
        'Transgender Male': 'Male',
        'Transgender Female': 'Female',
        'Other': 'Other',
        'Prefer Not to Say': 'Prefer not to say',
        'Prefer not to say': 'Prefer not to say',
    }
    return gender_map.get(csv_gender, None)


def map_marital_status(csv_status):
    """Map CSV marital status to entity enum"""
    status_map = {
        'Single': 'Single',
        'Married': 'Married',
        'Divorced': 'Divorced',
        'Widowed': 'Widowed',
        'Separated': 'Separated',
        'Prefer Not to Say': None,
        'Prefer not to say': None,
    }
    return status_map.get(csv_status, None)


def map_travel_method(csv_transport):
    """Map CSV transport mode to entity enum"""
    if not csv_transport:
        return None
    
    transport_lower = csv_transport.lower()
    
    if 'car' in transport_lower:
        return 'Car'
    elif 'bike' in transport_lower or 'bicycle' in transport_lower:
        return 'Bike'
    elif 'walk' in transport_lower:
        return 'Walk'
    elif 'public' in transport_lower or 'bus' in transport_lower or 'train' in transport_lower:
        return 'PublicTransport'
    
    return None


def clean_excel_value(value):
    """Clean Excel-formatted values like ="12345" """
    if not value:
        return ''
    
    value = str(value).strip()
    
    # Remove Excel formula formatting: ="value"
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]
    elif value.startswith('=') and value.endswith('"'):
        value = value[1:].strip('"')
    
    return value.strip()


def make_unique_email_placeholder(first_name, last_name, used_emails):
    """Generate unique placeholder email: firstname+lastname@AOSsystem.com (sanitized, unique)."""
    # Sanitize for email: lowercase, only keep chars valid in local part
    safe_first = re.sub(r'[^a-zA-Z0-9]', '', (first_name or ''))
    safe_last = re.sub(r'[^a-zA-Z0-9]', '', (last_name or ''))
    if not safe_first:
        safe_first = 'user'
    if not safe_last:
        safe_last = 'unknown'
    base = f"{safe_first}+{safe_last}@AOSsystem.com".lower()
    candidate = base
    suffix = 1
    while candidate in used_emails:
        suffix += 1
        candidate = f"{safe_first}+{safe_last}{suffix}@AOSsystem.com".lower()
    used_emails.add(candidate)
    return candidate


def extract_users_from_csv(csv_path, lookups):
    """Extract user data from CSV and map to database fields"""
    users = []
    used_emails = set()
    stats = {"skipped_no_name": 0, "skipped_terminated": 0, "warn_no_phone": 0, "placeholder_email": 0}
    
    logger.info("Reading CSV: %s", csv_path)
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row_num = 0
        
        for row in reader:
            row_num += 1
            
            # Basic fields first (need name for placeholder if email missing); fix encoding mojibake
            first_name = fix_utf8_mojibake(row.get('First Name', '') or '').strip()
            last_name = fix_utf8_mojibake(row.get('Last Name', '') or '').strip()

            # If Last Name is empty but First Name contains spaces, split
            if not last_name and first_name and ' ' in first_name:
                parts = first_name.split()
                titles = {'Mr', 'Mrs', 'Miss', 'Ms', 'Dr', 'Prof', 'Mr.', 'Mrs.', 'Miss.', 'Ms.', 'Dr.', 'Prof.'}
                while parts and parts[0] in titles:
                    parts = parts[1:]
                if len(parts) >= 2:
                    first_name = parts[0]
                    last_name = ' '.join(parts[1:])
                    logger.debug("Row %d: Split full name into first=%r, last=%r", row_num, first_name, last_name)
                else:
                    first_name = parts[0] if parts else ''
                    last_name = ''
            
            if not first_name or not last_name:
                stats["skipped_no_name"] += 1
                logger.warning(
                    "Row %d: SKIPPED - missing name | First Name=%r, Last Name=%r, Email=%r, Title=%r, Mobile=%r, Home=%r",
                    row_num, first_name, last_name, row.get('Email', ''), row.get('Title', ''), row.get('Mobile', ''), row.get('Home', '')
                )
                continue

            # Seed only when Termination Date is null/empty OR >= today (equal to today is seeded)
            termination_date = parse_date(clean_excel_value(row.get('Termination Date', '')))
            if termination_date is not None and termination_date < date.today():
                stats["skipped_terminated"] += 1
                logger.info(
                    "Row %d: SKIPPED - Termination Date %s is before today | name=%r, lastname=%r",
                    row_num, termination_date, first_name, last_name
                )
                continue

            # Email: use from CSV or unique placeholder when missing (fix encoding)
            email_raw = fix_utf8_mojibake(row.get('Email', '') or '').strip().lower()
            if email_raw:
                email = email_raw
                if email in used_emails:
                    logger.warning("Row %d: Duplicate email %r; may conflict on insert.", row_num, email)
                used_emails.add(email)
            else:
                email = make_unique_email_placeholder(first_name, last_name, used_emails)
                stats["placeholder_email"] += 1
                logger.info(
                    "Row %d: No email - using placeholder | email=%r | name=%r, lastname=%r",
                    row_num, email, first_name, last_name
                )
            
            # Phone number (prefer Mobile, fallback to Home) - clean Excel formatting
            mobile = clean_excel_value(row.get('Mobile', ''))
            home = clean_excel_value(row.get('Home', ''))
            phone = mobile or home
            
            if not phone or phone == '0':
                stats["warn_no_phone"] += 1
                logger.warning("Row %d: No valid phone number for %s", row_num, email)
                phone = ''
            
            # All users are caregivers
            is_caregiver = True
            
            # Map foreign keys
            title_id = None
            title_name = fix_utf8_mojibake(row.get('Title', '') or '').strip()
            if title_name and title_name in lookups['titles']:
                title_id = lookups['titles'][title_name]
            
            nationality_id = None
            nationality_name = fix_utf8_mojibake(row.get('Nationality', '') or '').strip()
            if nationality_name and nationality_name in lookups['nationalities']:
                nationality_id = lookups['nationalities'][nationality_name]

            religion_id = None
            religion_name = fix_utf8_mojibake(row.get('Religion', '') or '').strip()
            if religion_name and religion_name in lookups['religions']:
                religion_id = lookups['religions'][religion_name]

            origin_id = None
            origin_name = fix_utf8_mojibake(row.get('Ethnic Origin', '') or '').strip()
            if origin_name and origin_name in lookups['origins']:
                origin_id = lookups['origins'][origin_name]

            # Get group_id for many-to-many relationship (not area_id)
            group_id = None
            group_name = fix_utf8_mojibake(row.get('Area', '') or '').strip()
            if group_name and group_name in lookups['groups']:
                group_id = lookups['groups'][group_name]
            
            # Parse dates
            birth_date = parse_date(row.get('Date Of Birth', ''))
            
            # Map enums
            gender = map_gender(row.get('Gender', '').strip())
            marital_status = map_marital_status(row.get('MaritalStatus', '').strip())
            travel_method = map_travel_method(row.get('TransportModeDescription', ''))
            
            # Clean PPS Number
            pps_number = clean_excel_value(row.get('NI Number', '')) or None
            
            # Build user data
            user_data = {
                'name': first_name,
                'lastname': last_name,
                'middle_name': fix_utf8_mojibake(row.get('Initial', '') or '').strip() or None,
                'preferred_name': fix_utf8_mojibake(row.get('Preferred Name', '') or '').strip() or None,
                'email': email,
                'phone_number': phone,
                'password': 'default_password_123',
                'is_loginable': False,
                'is_caregiver': is_caregiver,
                'birth_date': birth_date,
                'gender': gender,
                'marital_status': marital_status,
                'pps_number': pps_number,
                'town': row.get('City / Town', '').strip() or None,
                'county': row.get('County', '').strip() or None,
                'postcode': row.get('Post Code', '').strip() or None,
                'travel_method': travel_method,
                'status': 'Active',  # Ignore CSV Status column; all seeded users are Active per termination-date filter
                'title_id': title_id,
                'nationality_id': nationality_id,
                'religion_id': religion_id,
                'origin_id': origin_id,
                'group_id': group_id,  # For many-to-many linking
            }
            
            users.append(user_data)
            logger.info(
                "Row %d: ADDED | email=%r, name=%r, lastname=%r, phone=%r, status=%s",
                row_num, email, first_name, last_name, phone or '(none)', user_data['status']
            )
    
    logger.info(
        "Extracted %d users from CSV. Skipped: %d (no name), %d (termination date before today). Placeholder email used: %d. Warnings (no phone): %d.",
        len(users), stats["skipped_no_name"], stats["skipped_terminated"], stats["placeholder_email"], stats["warn_no_phone"]
    )
    return users

def seed_users(connection, users, state=None):
    """Insert users into database and link to groups. If state is provided, use batched inserts and checkpoints."""
    if not users:
        logger.warning("No users to insert")
        return False

    # Deduplicate by email (last occurrence wins) so ON CONFLICT (email) never sees duplicates in one batch
    n_original = len(users)
    by_email = {u["email"]: u for u in users}
    users = list(by_email.values())
    if n_original > len(users):
        logger.info("Deduplicated by email: %d rows -> %d unique users (removed %d duplicate email(s))", n_original, len(users), n_original - len(users))

    required_keys = [
        'name', 'lastname', 'middle_name', 'preferred_name', 'email', 'phone_number',
        'password', 'is_loginable', 'is_caregiver', 'birth_date', 'gender', 'marital_status',
        'pps_number', 'town', 'county', 'postcode', 'travel_method', 'status',
        'title_id', 'nationality_id', 'religion_id', 'origin_id',
    ]
    user_tuples = []
    for idx, user in enumerate(users):
        try:
            for k in required_keys:
                if k not in user:
                    raise KeyError("missing key %r" % k)
            user_tuples.append((
                user['name'], user['lastname'], user['middle_name'], user['preferred_name'],
                user['email'], user['phone_number'], user['password'], user['is_loginable'],
                user['is_caregiver'], user['birth_date'], user['gender'], user['marital_status'],
                user['pps_number'], user['town'], user['county'], user['postcode'],
                user['travel_method'], user['status'], user['title_id'], user['nationality_id'],
                user['religion_id'], user['origin_id'],
            ))
        except KeyError as e:
            logger.error("User at index %d (email=%r): %s. Keys present: %s", idx, user.get('email'), e, list(user.keys()))
            raise ValueError("Invalid user at index %d: %s" % (idx, e)) from e

    if not user_tuples:
        logger.warning("No valid user tuples to insert after validation")
        return False

    insert_query = """
        INSERT INTO "user" (
            name, lastname, middle_name, preferred_name, email, phone_number,
            password, is_loginable, is_caregiver, birth_date, gender, marital_status,
            "ppsNumber", town, county, postcode, travel_method, status,
            title_id, nationality_id, religion_id, origin_id,
            created_date, last_modified_date
        ) VALUES %s
        ON CONFLICT (email) DO UPDATE SET
            name = EXCLUDED.name, lastname = EXCLUDED.lastname, middle_name = EXCLUDED.middle_name,
            preferred_name = EXCLUDED.preferred_name, phone_number = EXCLUDED.phone_number,
            is_loginable = EXCLUDED.is_loginable, is_caregiver = EXCLUDED.is_caregiver,
            birth_date = EXCLUDED.birth_date, gender = EXCLUDED.gender, marital_status = EXCLUDED.marital_status,
            "ppsNumber" = EXCLUDED."ppsNumber", town = EXCLUDED.town, county = EXCLUDED.county,
            postcode = EXCLUDED.postcode, travel_method = EXCLUDED.travel_method, status = EXCLUDED.status,
            title_id = EXCLUDED.title_id, nationality_id = EXCLUDED.nationality_id,
            religion_id = EXCLUDED.religion_id, origin_id = EXCLUDED.origin_id,
            last_modified_date = NOW()
        RETURNING id, email
    """
    template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"

    cursor = connection.cursor()
    try:
        if state is None:
            execute_values(cursor, insert_query, user_tuples, template=template, fetch=True)
            processed = cursor.fetchall()
            if processed:
                user_ids = [row['id'] for row in processed]
                cursor.execute("DELETE FROM user_users_groups WHERE user_id = ANY(%s)", (user_ids,))
                link_users_to_groups(cursor, processed, users)
            connection.commit()
            logger.info("Successfully processed %d users (inserted/updated)", len(processed))
            return True
        start_batch = state.get("users_migration", "batch_index", 0)
        batch_size = USER_BATCH_SIZE
        all_processed = []
        total_committed = start_batch * batch_size
        for batch_start in range(start_batch * batch_size, len(user_tuples), batch_size):
            batch = user_tuples[batch_start:batch_start + batch_size]
            batch_index = batch_start // batch_size
            try:
                execute_values(cursor, insert_query, batch, template=template, fetch=True)
                processed = cursor.fetchall()
                all_processed.extend(processed)
                if processed:
                    user_ids = [row['id'] for row in processed]
                    cursor.execute("DELETE FROM user_users_groups WHERE user_id = ANY(%s)", (user_ids,))
                    batch_users = users[batch_start:batch_start + len(batch)]
                    link_users_to_groups(cursor, processed, batch_users)
                connection.commit()
                total_committed = batch_start + len(batch)
                state.update("users_migration", status="in_progress", batch_index=batch_index + 1, rows_committed=total_committed)
            except (OperationalError, InterfaceError) as e:
                connection.rollback()
                if ConnectionLostError:
                    raise ConnectionLostError("users_migration", dict(batch_index=batch_index)) from e
                raise

        state.update("users_migration", status="completed", rows_committed=total_committed)
        state.clear_step("users_migration")
        logger.info("Successfully processed %d users (inserted/updated)", len(all_processed))
        return True
    except (OperationalError, InterfaceError):
        connection.rollback()
        raise
    except Exception as e:
        connection.rollback()
        logger.error("\nFailed to process users: %s", e)
        raise
    finally:
        cursor.close()

def link_users_to_groups(cursor, inserted_users, original_users):
    """Link users to their groups via many-to-many relationship"""
    
    # Create email to user mapping
    email_to_user = {user['email']: user for user in original_users}
    
    # Prepare user-group links
    user_group_links = []
    for user_row in inserted_users:
        email = user_row['email']
        user_id = user_row['id']
        
        # Get group_id from original user data
        original_user = email_to_user.get(email)
        if original_user and original_user.get('group_id'):
            group_id = original_user['group_id']
            user_group_links.append((user_id, group_id))
    
    if not user_group_links:
        logger.warning("No user-group links to create")
        return
    
    # Insert into user_users_groups junction table
    insert_groups_query = """
        INSERT INTO user_users_groups (user_id, group_id)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    
    execute_values(
        cursor,
        insert_groups_query,
        user_group_links,
        template="(%s, %s)"
    )
    
    logger.info(f"✓ Linked {len(user_group_links)} user-group relationships")


def run(connection_manager=None, state=None):
    """Main execution function. When run from wizard, connection_manager and state are provided for resume support."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Users Migration                                  ║
    ║         Seeding user table from CSV                      ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    if state and state.is_completed("users_migration"):
        logger.info("Users migration already completed (resume).")
        return True
    config = get_db_config()
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in .env file")
        logger.error("Required: DB_NAME, DB_USER, DB_PASSWORD")
        return False
    csv_path = get_assets_dir() / 'CareAssistantExport.csv'
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return False
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
        logger.info("STEP 2: LOAD LOOKUP TABLES")
        logger.info("="*60)
        lookups = get_lookup_tables(connection)
        logger.info("\n" + "="*60)
        logger.info("STEP 3: EXTRACT USERS FROM CSV")
        logger.info("="*60)
        users = extract_users_from_csv(csv_path, lookups)
        if not users:
            logger.warning("No users found in CSV")
            return False
        logger.info("\n" + "="*60)
        logger.info("STEP 4: SEED USERS TO DATABASE")
        logger.info("="*60)
        success = seed_users(connection, users, state=state)
        if success:
            print("\n" + "="*60)
            print("✓ USERS MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            return True
        print("\n" + "="*60)
        print("✗ USERS MIGRATION FAILED")
        print("="*60)
        return False
    except (OperationalError, InterfaceError) as e:
        if ConnectionLostError and not connection_manager:
            raise ConnectionLostError("users_migration", {}) from e
        raise
    except Exception as e:
        logger.error(f"Migration error: {e}", exc_info=True)
        return False
    finally:
        if connection and not connection_manager:
            connection.close()
            logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    import sys
    success = run()
    sys.exit(0 if success else 1)