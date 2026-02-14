"""
Users Migration
Seeds user table from CSV and links to groups
"""

import os
import csv
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

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
            cursor_factory=RealDictCursor
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
    """Parse date from CSV format DD/MM/YYYY HH:MM:SS"""
    if not date_str or date_str.strip() == '':
        return None
    
    try:
        # Format: 01/01/1999 00:00:00
        return datetime.strptime(date_str.strip(), '%d/%m/%Y %H:%M:%S').date()
    except:
        try:
            # Try without time
            return datetime.strptime(date_str.strip(), '%d/%m/%Y').date()
        except:
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


def extract_users_from_csv(csv_path, lookups):
    """Extract user data from CSV and map to database fields"""
    users = []
    stats = {"skipped_no_email": 0, "skipped_no_name": 0, "warn_no_phone": 0}
    
    logger.info("Reading CSV: %s", csv_path)
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row_num = 0
        
        for row in reader:
            row_num += 1
            
            # Skip if no email (required field)
            email = row.get('Email', '').strip().lower()
            if not email:
                stats["skipped_no_email"] += 1
                logger.warning("Row %d: Skipping - no email", row_num)
                continue
            
            # Basic fields
            first_name = row.get('First Name', '').strip()
            last_name = row.get('Last Name', '').strip()
            
            if not first_name or not last_name:
                stats["skipped_no_name"] += 1
                logger.warning("Row %d: Skipping - missing name", row_num)
                continue
            
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
            title_name = row.get('Title', '').strip()
            if title_name and title_name in lookups['titles']:
                title_id = lookups['titles'][title_name]
            
            nationality_id = None
            nationality_name = row.get('Nationality', '').strip()
            if nationality_name and nationality_name in lookups['nationalities']:
                nationality_id = lookups['nationalities'][nationality_name]
            
            religion_id = None
            religion_name = row.get('Religion', '').strip()
            if religion_name and religion_name in lookups['religions']:
                religion_id = lookups['religions'][religion_name]
            
            origin_id = None
            origin_name = row.get('Ethnic Origin', '').strip()
            if origin_name and origin_name in lookups['origins']:
                origin_id = lookups['origins'][origin_name]
            
            # Get group_id for many-to-many relationship (not area_id)
            group_id = None
            group_name = row.get('Area', '').strip()
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
                'middle_name': row.get('Initial', '').strip() or None,
                'preferred_name': row.get('Preferred Name', '').strip() or None,
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
                'status': 'Active' if row.get('Status', '').strip() == 'Active' else 'Deactive',
                'title_id': title_id,
                'nationality_id': nationality_id,
                'religion_id': religion_id,
                'origin_id': origin_id,
                'group_id': group_id,  # For many-to-many linking
            }
            
            users.append(user_data)
    
    logger.info(
        "Extracted %d users from CSV. Skipped: %d (no email), %d (no name). Warnings: %d (no phone).",
        len(users), stats["skipped_no_email"], stats["skipped_no_name"], stats["warn_no_phone"]
    )
    return users

def seed_users(connection, users):
    """Insert users into database and link to groups"""
    if not users:
        logger.warning("No users to insert")
        return False
    
    logger.info(f"Inserting/Updating {len(users)} users...")
    
    cursor = connection.cursor()
    try:
        # Resolve sequence for user.id (table may have no DEFAULT, causing NOT NULL violation)
        cursor.execute(
            """SELECT pg_get_serial_sequence('"user"', 'id') AS seq_name"""
        )
        row = cursor.fetchone()
        seq_name = (row and row.get("seq_name")) or "user_id_seq"
        seq_name = str(seq_name).strip() if seq_name else "user_id_seq"
        # Ensure sequence exists (table may have no DEFAULT; create if missing)
        try:
            cursor.execute("CREATE SEQUENCE IF NOT EXISTS user_id_seq")
        except Exception as e:
            logger.debug("Sequence user_id_seq create/skip: %s", e)
        # nextval() needs a regclass; quote if it contains a schema (e.g. public.user_id_seq)
        seq_sql = "nextval('%s'::regclass)" % seq_name.replace("'", "''")
        # Build insert query: include id via nextval(seq) so each row gets a generated id
        insert_query = """
            INSERT INTO "user" (
                id, name, lastname, middle_name, preferred_name, email, phone_number,
                password, is_loginable, is_caregiver, birth_date, gender, marital_status,
                "ppsNumber", town, county, postcode, travel_method, status,
                title_id, nationality_id, religion_id, origin_id,
                created_date, last_modified_date
            ) VALUES %s
            ON CONFLICT (email) DO UPDATE SET
                name = EXCLUDED.name,
                lastname = EXCLUDED.lastname,
                middle_name = EXCLUDED.middle_name,
                preferred_name = EXCLUDED.preferred_name,
                phone_number = EXCLUDED.phone_number,
                is_loginable = EXCLUDED.is_loginable,
                is_caregiver = EXCLUDED.is_caregiver,
                birth_date = EXCLUDED.birth_date,
                gender = EXCLUDED.gender,
                marital_status = EXCLUDED.marital_status,
                "ppsNumber" = EXCLUDED."ppsNumber",
                town = EXCLUDED.town,
                county = EXCLUDED.county,
                postcode = EXCLUDED.postcode,
                travel_method = EXCLUDED.travel_method,
                status = EXCLUDED.status,
                title_id = EXCLUDED.title_id,
                nationality_id = EXCLUDED.nationality_id,
                religion_id = EXCLUDED.religion_id,
                origin_id = EXCLUDED.origin_id,
                last_modified_date = NOW()
            RETURNING id, email
        """
        
        # Required keys for each user (must match tuple order and template)
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
                    user['name'],
                    user['lastname'],
                    user['middle_name'],
                    user['preferred_name'],
                    user['email'],
                    user['phone_number'],
                    user['password'],
                    user['is_loginable'],
                    user['is_caregiver'],
                    user['birth_date'],
                    user['gender'],
                    user['marital_status'],
                    user['pps_number'],
                    user['town'],
                    user['county'],
                    user['postcode'],
                    user['travel_method'],
                    user['status'],
                    user['title_id'],
                    user['nationality_id'],
                    user['religion_id'],
                    user['origin_id'],
                ))
            except KeyError as e:
                logger.error("User at index %d (email=%r): %s. Keys present: %s", idx, user.get('email'), e, list(user.keys()))
                raise ValueError("Invalid user at index %d: %s" % (idx, e)) from e

        if not user_tuples:
            logger.warning("No valid user tuples to insert after validation")
            return False

        logger.info("Inserting %d user rows (id=nextval + 22 fields + created_date, last_modified_date)...", len(user_tuples))
        # 25 columns: id (nextval), 22 from tuple, created_date, last_modified_date. Template must have exactly 22 %%s to match tuple length.
        template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
        template_with_id = "(" + seq_sql + ", " + template[1:]
        execute_values(
            cursor,
            insert_query,
            user_tuples,
            template=template_with_id,
            fetch=True,
        )
        
        processed = cursor.fetchall()
        
        # Link users to groups (many-to-many)
        # First, clear existing group links for these users
        if processed:
            logger.info(f"\nUpdating group links for {len(processed)} users...")
            user_ids = [row['id'] for row in processed]
            
            # Delete existing group links
            cursor.execute(
                "DELETE FROM user_users_groups WHERE user_id = ANY(%s)",
                (user_ids,)
            )
            
            # Re-link users to groups
            link_users_to_groups(cursor, processed, users)
        
        connection.commit()
        
        logger.info(f"\n✓ Successfully processed {len(processed)} users (inserted/updated)")
        
        # Show sample of processed users
        logger.info("\nSample processed users:")
        for i, row in enumerate(processed[:5]):
            logger.info(f"  - {row['email']} (ID: {row['id']})")
        if len(processed) > 5:
            logger.info(f"  ... and {len(processed) - 5} more")
        
        return True
        
    except Exception as e:
        connection.rollback()
        logger.error(f"\n✗ Failed to process users: {e}")
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


def run():
    """Main execution function"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Users Migration                                  ║
    ║         Seeding user table from CSV                      ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Get database config
    config = get_db_config()
    
    # Validate config
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in .env file")
        logger.error("Required: DB_NAME, DB_USER, DB_PASSWORD")
        return False
    
    # Get CSV path
    csv_path = Path(__file__).parent.parent / 'assets' / 'CareAssistantExport.csv'
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return False
    
    # Connect and migrate
    connection = None
    try:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: DATABASE CONNECTION")
        logger.info("="*60)
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
        success = seed_users(connection, users)
        
        if success:
            print("\n" + "="*60)
            print("✓ USERS MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            return True
        else:
            print("\n" + "="*60)
            print("✗ USERS MIGRATION FAILED")
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
    import sys
    success = run()
    sys.exit(0 if success else 1)