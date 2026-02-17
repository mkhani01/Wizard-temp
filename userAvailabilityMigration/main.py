"""
User Availability Migration Script
===================================
Migrates user availability data from Excel file to database.

Rules:
- Only records with "Type" = "Core" are processed
- Care Assistant Name has titles (Mr, Mrs, Miss, Ms, Dr, Prof) - stripped to match user
- Start Date - 28 days = start_date for seeding (recurring availability)
- occurs_every = 4 (monthly recurrence - first week of each month)
- "Core" availability type must exist in database (throws error if not found)
- is_temp = False (recurring), is_unavailability = False
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta, date, time
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor, execute_values
    import openpyxl
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Please install: pip install psycopg2-binary openpyxl")
    sys.exit(1)


# Configure comprehensive logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('migration_core_availability.log', mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# Excel column indices (0-based) based on the header:
# A(0): Care Assistant Name
# B(1): Care Assistant Franchise
# C(2): Care Assistant Team
# D(3): Care Assistant Type
# E(4): Grade Type
# F(5): Start Date
# G(6): Start Time
# H(7): End Date
# I(8): End Time
# J(9): Hours
# K(10): Type
# L(11): Notes

COLUMN_CARE_ASSISTANT_NAME = 0
COLUMN_FRANCHISE = 1
COLUMN_TEAM = 2
COLUMN_CARE_ASSISTANT_TYPE = 3
COLUMN_GRADE_TYPE = 4
COLUMN_START_DATE = 5
COLUMN_START_TIME = 6
COLUMN_END_DATE = 7
COLUMN_END_TIME = 8
COLUMN_HOURS = 9
COLUMN_TYPE = 10
COLUMN_NOTES = 11

# Days of week enum values (must match DayOfWeek enum in entity)
DAYS_OF_WEEK = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# Titles to strip from names
TITLES = ['Mr', 'Mrs', 'Miss', 'Ms', 'Dr', 'Prof', 'Mr.', 'Mrs.', 'Miss.', 'Ms.', 'Dr.', 'Prof.']


class MigrationError(Exception):
    """Custom exception for migration errors"""
    pass


def get_db_config() -> Dict[str, Any]:
    """Get database configuration from environment variables"""
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise MigrationError(f"Missing database configuration: {missing}")
    
    return config


def connect_to_database(config: Dict[str, Any]):
    """Connect to PostgreSQL database"""
    try:
        logger.info(f"Connecting to PostgreSQL at {config['host']}:{config['port']}/{config['database']}...")
        connection = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password'],
            cursor_factory=RealDictCursor
        )
        connection.autocommit = False
        logger.info("✓ Database connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"✗ Failed to connect to database: {e}")
        raise MigrationError(f"Database connection failed: {e}")


def get_all_users(connection) -> Dict[str, int]:
    """Get all users from database mapped by 'name lastname' (lowercase)"""
    cursor = connection.cursor()
    try:
        cursor.execute('SELECT id, name, lastname FROM "user" WHERE deleted_at IS NULL')
        users = {}
        for row in cursor.fetchall():
            name = (row['name'] or '').strip()
            lastname = (row['lastname'] or '').strip()
            key = f"{name} {lastname}".strip().lower()
            if key:
                users[key] = row['id']
        
        logger.info(f"✓ Loaded {len(users)} users from database")
        logger.debug(f"Sample user keys: {list(users.keys())[:5]}")
        return users
    finally:
        cursor.close()


def get_core_availability_type(connection) -> Tuple[int, bool]:
    """
    Get the 'Core' availability type from database.
    Returns (type_id, is_unavailability).
    Throws error if not found.
    """
    cursor = connection.cursor()
    try:
        # Try to find 'Core' type (case-insensitive)
        cursor.execute(
            "SELECT id, name, type FROM availability_types "
            "WHERE LOWER(name) = 'core' AND deleted_at IS NULL"
        )
        row = cursor.fetchone()
        
        if not row:
            # Log all available types for debugging
            cursor.execute("SELECT id, name, type FROM availability_types WHERE deleted_at IS NULL")
            all_types = cursor.fetchall()
            logger.error("Available availability types in database:")
            for t in all_types:
                logger.error(f"  - ID: {t['id']}, Name: '{t['name']}', Type: '{t['type']}'")
            
            raise MigrationError(
                "CRITICAL: 'Core' availability type not found in database. "
                "Please ensure the 'Core' availability type is seeded before running this migration."
            )
        
        type_id = row['id']
        is_unavailability = (row['type'] or '').strip().lower() == 'unavailability'
        
        logger.info(f"✓ Found 'Core' availability type: ID={type_id}, is_unavailability={is_unavailability}")
        return type_id, is_unavailability
        
    finally:
        cursor.close()


def strip_title(full_name: str) -> str:
    """Strip title from name and return cleaned name"""
    if not full_name:
        return ''
    
    parts = full_name.strip().split()
    if not parts:
        return ''
    
    # Remove title from beginning
    while parts and parts[0] in TITLES:
        parts = parts[1:]
    
    return ' '.join(parts)


def parse_time_value(time_val) -> Optional[time]:
    """Parse time from various formats (datetime.time, string, etc.)"""
    if time_val is None:
        return None
    
    if isinstance(time_val, time):
        return time_val
    
    if isinstance(time_val, datetime):
        return time_val.time()
    
    if isinstance(time_val, str):
        try:
            # Try HH:MM:SS format
            parts = time_val.split(':')
            if len(parts) >= 2:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2]) if len(parts) > 2 else 0
                return time(hours, minutes, seconds)
        except (ValueError, IndexError):
            pass
    
    return None


def parse_date_value(date_val) -> Optional[date]:
    """Parse date from various formats (datetime, date, string)"""
    if date_val is None:
        return None
    
    if isinstance(date_val, datetime):
        return date_val.date()
    
    if isinstance(date_val, date):
        return date_val
    
    if isinstance(date_val, str):
        date_str = date_val.strip()
        # Try DD/MM/YYYY format (common in UK/EU)
        for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
    
    return None


def get_day_of_week(date_obj: date) -> str:
    """Get day of week name from date (Monday=0 to Sunday=6)"""
    return DAYS_OF_WEEK[date_obj.weekday()]


def format_time_str(t: time) -> str:
    """Format time as HH:MM:SS string"""
    return t.strftime('%H:%M:%S')


def format_date_str(d: date) -> str:
    """Format date as YYYY-MM-DD string"""
    return d.strftime('%Y-%m-%d')


def process_xlsx_file(filepath: Path, users_map: Dict[str, int]) -> Tuple[List[Dict], List[str]]:
    """
    Process the Excel file and extract Core availability records.
    
    Returns:
        Tuple of (valid_records, unmatched_users)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"PROCESSING EXCEL FILE: {filepath}")
    logger.info(f"{'='*60}")
    
    if not filepath.exists():
        raise MigrationError(f"Excel file not found: {filepath}")
    
    try:
        wb = openpyxl.load_workbook(filepath, data_only=True)
    except Exception as e:
        raise MigrationError(f"Failed to load Excel file: {e}")
    
    # Check for expected sheet
    sheet_name = 'Care Assistant Availability'
    if sheet_name not in wb.sheetnames:
        available_sheets = wb.sheetnames
        logger.warning(f"Sheet '{sheet_name}' not found. Available sheets: {available_sheets}")
        # Try first sheet as fallback
        sheet_name = wb.sheetnames[0]
        logger.info(f"Using sheet: {sheet_name}")
    
    ws = wb[sheet_name]
    
    # Log header row for debugging
    header_row = list(ws.iter_rows(min_row=1, max_row=1, values_only=True))[0]
    logger.info(f"Header row: {header_row}")
    
    valid_records = []
    unmatched_users = set()
    skipped_non_core = 0
    skipped_missing_data = 0
    total_rows = 0
    
    # Process data rows (skip header)
    for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        total_rows += 1
        
        # Extract values from row
        care_assistant_name = row[COLUMN_CARE_ASSISTANT_NAME] if len(row) > COLUMN_CARE_ASSISTANT_NAME else None
        start_date_val = row[COLUMN_START_DATE] if len(row) > COLUMN_START_DATE else None
        start_time_val = row[COLUMN_START_TIME] if len(row) > COLUMN_START_TIME else None
        end_time_val = row[COLUMN_END_TIME] if len(row) > COLUMN_END_TIME else None
        type_val = row[COLUMN_TYPE] if len(row) > COLUMN_TYPE else None
        
        # Skip empty rows
        if not care_assistant_name:
            continue
        
        # Normalize type value
        type_str = str(type_val).strip() if type_val else ''
        
        # ONLY process "Core" records
        if type_str.lower() != 'core':
            skipped_non_core += 1
            logger.debug(f"Row {row_num}: Skipping non-Core record (Type='{type_str}')")
            continue
        
        # Strip title and match user
        name_without_title = strip_title(str(care_assistant_name))
        user_key = name_without_title.strip().lower()
        
        user_id = users_map.get(user_key)
        if not user_id:
            unmatched_users.add(str(care_assistant_name))
            logger.warning(f"Row {row_num}: User not found - '{care_assistant_name}' (key: '{user_key}')")
            continue
        
        # Parse dates and times
        start_date = parse_date_value(start_date_val)
        start_time = parse_time_value(start_time_val)
        end_time = parse_time_value(end_time_val)
        
        # Validate required fields
        if not all([start_date, start_time, end_time]):
            skipped_missing_data += 1
            logger.warning(
                f"Row {row_num}: Missing required data for user '{care_assistant_name}' - "
                f"start_date={start_date}, start_time={start_time}, end_time={end_time}"
            )
            continue
        
        # Calculate target start_date (Start Date - 28 days)
        target_start_date = start_date - timedelta(days=28)
        
        valid_records.append({
            'user_id': user_id,
            'user_name': care_assistant_name,
            'original_start_date': start_date,
            'target_start_date': target_start_date,
            'start_time': start_time,
            'end_time': end_time,
            'day_of_week': get_day_of_week(target_start_date),
            'source_row': row_num,
        })
        
        logger.debug(
            f"Row {row_num}: Valid record - User ID={user_id}, "
            f"Original Date={format_date_str(start_date)}, "
            f"Target Date={format_date_str(target_start_date)}, "
            f"Time={format_time_str(start_time)}-{format_time_str(end_time)}"
        )
    
    logger.info(f"\n{'='*60}")
    logger.info("EXCEL PROCESSING SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total rows processed: {total_rows}")
    logger.info(f"Valid Core records extracted: {len(valid_records)}")
    logger.info(f"Skipped (non-Core type): {skipped_non_core}")
    logger.info(f"Skipped (missing data): {skipped_missing_data}")
    logger.info(f"Unmatched users: {len(unmatched_users)}")
    
    return valid_records, list(unmatched_users)


def generate_availability_records(
    records: List[Dict], 
    core_type_id: int,
    is_unavailability: bool
) -> List[Dict]:
    """
    Generate availability records for database insertion.
    
    Rules:
    - is_temp = False (recurring availability)
    - start_date = target_start_date (original date - 28 days)
    - end_date = None (no end date)
    - occurs_every = 4 (monthly recurrence)
    - days = [day_of_week of target_start_date]
    """
    logger.info(f"\n{'='*60}")
    logger.info("GENERATING AVAILABILITY RECORDS")
    logger.info(f"{'='*60}")
    
    availabilities = []
    
    for record in records:
        user_id = record['user_id']
        target_start_date = record['target_start_date']
        start_time = record['start_time']
        end_time = record['end_time']
        day_of_week = record['day_of_week']
        
        # Handle overnight shifts (end_time < start_time)
        if end_time < start_time:
            logger.warning(
                f"Overnight shift detected for user {user_id}: "
                f"{format_time_str(start_time)}-{format_time_str(end_time)}. "
                f"Splitting into two records."
            )
            
            # First part: start_time to 23:59:59
            availabilities.append({
                'user_id': user_id,
                'days': [day_of_week],
                'start_time': format_time_str(start_time),
                'end_time': '23:59:59',
                'is_temp': False,
                'is_unavailability': is_unavailability,
                'type_id': core_type_id,
                'start_date': format_date_str(target_start_date),
                'end_date': None,
                'occurs_every': 4,
                'effective_date_from': None,
                'effective_date_to': None,
            })
            
            # Second part: 00:00:00 to end_time (next day)
            next_day = target_start_date + timedelta(days=1)
            next_day_of_week = get_day_of_week(next_day)
            
            availabilities.append({
                'user_id': user_id,
                'days': [next_day_of_week],
                'start_time': '00:00:00',
                'end_time': format_time_str(end_time),
                'is_temp': False,
                'is_unavailability': is_unavailability,
                'type_id': core_type_id,
                'start_date': format_date_str(next_day),
                'end_date': None,
                'occurs_every': 4,
                'effective_date_from': None,
                'effective_date_to': None,
            })
        else:
            # Normal shift
            availabilities.append({
                'user_id': user_id,
                'days': [day_of_week],
                'start_time': format_time_str(start_time),
                'end_time': format_time_str(end_time),
                'is_temp': False,
                'is_unavailability': is_unavailability,
                'type_id': core_type_id,
                'start_date': format_date_str(target_start_date),
                'end_date': None,
                'occurs_every': 4,
                'effective_date_from': None,
                'effective_date_to': None,
            })
    
    logger.info(f"Generated {len(availabilities)} availability records")
    return availabilities


def deduplicate_availabilities(availabilities: List[Dict]) -> List[Dict]:
    """Remove duplicate availabilities based on user_id, days, start_time, end_time, start_date"""
    unique_map = {}
    duplicates = 0
    
    for avail in availabilities:
        key = (
            avail['user_id'],
            tuple(avail['days']),
            avail['start_time'],
            avail['end_time'],
            avail['start_date'],
        )
        
        if key in unique_map:
            duplicates += 1
            logger.debug(f"Duplicate found: {key}")
        else:
            unique_map[key] = avail
    
    logger.info(f"Deduplication: {len(availabilities)} -> {len(unique_map)} records ({duplicates} duplicates removed)")
    return list(unique_map.values())


def seed_availabilities(connection, availabilities: List[Dict]) -> int:
    """Insert user availabilities into database"""
    if not availabilities:
        logger.warning("No availabilities to insert")
        return 0
    
    logger.info(f"\n{'='*60}")
    logger.info("SEEDING DATABASE")
    logger.info(f"{'='*60}")
    
    cursor = connection.cursor()
    try:
        # First, verify the enum type exists
        cursor.execute(
            "SELECT typname FROM pg_type WHERE typname = 'user_availabilities_days_enum'"
        )
        enum_exists = cursor.fetchone()
        
        if not enum_exists:
            logger.warning("Enum type 'user_availabilities_days_enum' not found. Using text array cast.")
            array_cast = '::text[]'
        else:
            array_cast = '::text[]::user_availabilities_days_enum[]'
        
        insert_query = f"""
            INSERT INTO user_availabilities (
                user_id, days, start_time, end_time, 
                is_temp, is_unavailability, type_id,
                start_date, end_date, occurs_every, 
                effective_date_from, effective_date_to,
                created_date, last_modified_date
            ) VALUES %s
            RETURNING id
        """
        
        availability_tuples = [
            (
                avail['user_id'],
                avail['days'],
                avail['start_time'],
                avail['end_time'],
                avail['is_temp'],
                avail['is_unavailability'],
                avail['type_id'],
                avail['start_date'],
                avail['end_date'],
                avail['occurs_every'],
                avail['effective_date_from'],
                avail['effective_date_to'],
            )
            for avail in availabilities
        ]
        
        logger.info(f"Inserting {len(availability_tuples)} records...")
        
        execute_values(
            cursor,
            insert_query,
            availability_tuples,
            template=f"(%s, %s{array_cast}, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
        )
        
        inserted = cursor.fetchall()
        connection.commit()
        
        logger.info(f"✓ Successfully inserted {len(inserted)} availability records")
        return len(inserted)
        
    except Exception as e:
        connection.rollback()
        logger.error(f"✗ Failed to insert availabilities: {e}")
        raise MigrationError(f"Database insertion failed: {e}")
    finally:
        cursor.close()


def generate_summary_report(
    records: List[Dict],
    availabilities: List[Dict],
    unmatched_users: List[str],
    inserted_count: int
) -> str:
    """Generate a summary report of the migration"""
    report = []
    report.append("\n" + "="*70)
    report.append("MIGRATION SUMMARY REPORT")
    report.append("="*70)
    
    report.append(f"\n📊 INPUT DATA:")
    report.append(f"   - Valid Core records extracted: {len(records)}")
    report.append(f"   - Unmatched users: {len(unmatched_users)}")
    
    if unmatched_users:
        report.append(f"\n⚠️  UNMATCHED USERS ({len(unmatched_users)}):")
        for user in sorted(unmatched_users):
            report.append(f"   - {user}")
    
    report.append(f"\n📋 GENERATED RECORDS:")
    report.append(f"   - Total availability records: {len(availabilities)}")
    
    # Group by user
    by_user = defaultdict(int)
    for a in availabilities:
        by_user[a['user_id']] += 1
    
    report.append(f"   - Unique users: {len(by_user)}")
    report.append(f"   - Records per user (avg): {len(availabilities)/len(by_user):.1f}" if by_user else "   - Records per user: N/A")
    
    report.append(f"\n✅ DATABASE:")
    report.append(f"   - Records inserted: {inserted_count}")
    
    # Sample records
    report.append(f"\n📝 SAMPLE RECORDS (first 5):")
    for i, a in enumerate(availabilities[:5]):
        report.append(f"   {i+1}. User ID: {a['user_id']}, Day: {a['days']}, "
                     f"Time: {a['start_time']}-{a['end_time']}, "
                     f"Start Date: {a['start_date']}, Occurs Every: {a['occurs_every']} weeks")
    
    report.append("\n" + "="*70)
    
    return "\n".join(report)


def run(xlsx_path: Optional[str] = None) -> bool:
    """Main execution function"""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║   USER AVAILABILITY MIGRATION - CORE RECORDS ONLY            ║
    ║   Rules:                                                     ║
    ║   - Only "Core" type records are processed                   ║
    ║   - Start Date - 28 days = recurring start_date              ║
    ║   - occurs_every = 4 (monthly - first week of each month)    ║
    ║   - is_temp = False (recurring availability)                 ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Determine Excel file path
    if xlsx_path:
        filepath = Path(xlsx_path)
    else:
        # Default path
        from migration_support import get_assets_dir
        filepath = get_assets_dir() / 'userAvailabilities' / 'userAvailabilities.xlsx'
    
    logger.info(f"Excel file path: {filepath}")
    
    connection = None
    
    try:
        # Step 1: Database connection
        logger.info(f"\n{'='*60}")
        logger.info("STEP 1: DATABASE CONNECTION")
        logger.info(f"{'='*60}")
        
        config = get_db_config()
        connection = connect_to_database(config)
        
        # Step 2: Load reference data
        logger.info(f"\n{'='*60}")
        logger.info("STEP 2: LOAD REFERENCE DATA")
        logger.info(f"{'='*60}")
        
        users_map = get_all_users(connection)
        core_type_id, is_unavailability = get_core_availability_type(connection)
        
        # Step 3: Process Excel file
        logger.info(f"\n{'='*60}")
        logger.info("STEP 3: PROCESS EXCEL FILE")
        logger.info(f"{'='*60}")
        
        records, unmatched_users = process_xlsx_file(filepath, users_map)
        
        if not records:
            logger.error("No valid Core records found in Excel file")
            return False
        
        # Step 4: Generate availability records
        logger.info(f"\n{'='*60}")
        logger.info("STEP 4: GENERATE AVAILABILITY RECORDS")
        logger.info(f"{'='*60}")
        
        availabilities = generate_availability_records(records, core_type_id, is_unavailability)
        availabilities = deduplicate_availabilities(availabilities)
        
        # Step 5: Seed database
        logger.info(f"\n{'='*60}")
        logger.info("STEP 5: SEED DATABASE")
        logger.info(f"{'='*60}")
        
        inserted_count = seed_availabilities(connection, availabilities)
        
        # Generate summary report
        report = generate_summary_report(records, availabilities, unmatched_users, inserted_count)
        print(report)
        
        # Log to file
        logger.info(report)
        
        print("\n" + "="*60)
        print("✓ MIGRATION COMPLETED SUCCESSFULLY")
        print("="*60)
        
        return True
        
    except MigrationError as e:
        logger.error(f"Migration error: {e}")
        print(f"\n✗ MIGRATION FAILED: {e}")
        return False
        
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        print(f"\n✗ MIGRATION FAILED: {e}")
        return False
        
    finally:
        if connection:
            connection.close()
            logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    # Allow passing Excel file path as argument
    xlsx_arg = sys.argv[1] if len(sys.argv) > 1 else None
    
    success = run(xlsx_arg)
    sys.exit(0 if success else 1)