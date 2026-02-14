"""
User Availability Migration - January & February 2026
Analyzes March 2026 data and generates availability records by subtracting 4 weeks (28 days).
- Core → recurring (is_temp=False): start_date, end_date, occurs_every=4.
- All other availability types → temporary (is_temp=True): effective_date_from, effective_date_to.
- type_id and is_unavailability are resolved from availability_types table (by type name).
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta, time, date
from collections import defaultdict
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import openpyxl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_jan2026_from_march.log'),
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


def get_all_users(connection):
    """Get all users from database"""
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id, name, lastname FROM \"user\"")
        users = {}
        for row in cursor.fetchall():
            key = f"{row['name']} {row['lastname']}".strip().lower()
            users[key] = row['id']
        
        logger.info(f"Loaded {len(users)} users from database")
        return users
    finally:
        cursor.close()


# Excel column indices (0-based) - based on header row:
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
# K(10): Type  <-- This is the availability type name (e.g., "Core", "Office availability")
# L(11): Notes

CARE_ASSISTANT_NAME_COLUMN = 0
FRANCHISE_COLUMN = 1
TEAM_COLUMN = 2
CARE_ASSISTANT_TYPE_COLUMN = 3
GRADE_TYPE_COLUMN = 4
START_DATE_COLUMN = 5
START_TIME_COLUMN = 6
END_DATE_COLUMN = 7
END_TIME_COLUMN = 8
HOURS_COLUMN = 9
TYPE_NAME_COLUMN_INDEX = 10  # "Type" column - availability type name
NOTES_COLUMN = 11


def get_availability_types(connection):
    """
    Load availability_types from DB: name -> {id, is_unavailability}.
    is_unavailability is True when type enum is 'unavailability'.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT id, name, type FROM availability_types WHERE deleted_at IS NULL"
        )
        result = {}
        for row in cursor.fetchall():
            name = (row['name'] or '').strip()
            if not name:
                continue
            result[name.lower()] = {
                'id': row['id'],
                'is_unavailability': (row['type'] or '').strip().lower() == 'unavailability',
            }
        logger.info(f"Loaded {len(result)} availability types from database")
        return result
    finally:
        cursor.close()


def strip_title(full_name):
    """Strip title from name"""
    titles = ['Mr', 'Mrs', 'Miss', 'Ms', 'Dr', 'Prof']
    parts = full_name.strip().split()
    if parts and parts[0] in titles:
        return ' '.join(parts[1:])
    return full_name


def parse_time_value(time_val):
    """Parse time from various formats"""
    if isinstance(time_val, time):
        return time_val
    if isinstance(time_val, str):
        try:
            parts = time_val.split(':')
            return time(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)
        except:
            return None
    return None


def parse_date_value(date_val):
    """Parse date from various formats"""
    if isinstance(date_val, datetime):
        return date_val.date()
    if isinstance(date_val, date):
        return date_val
    if isinstance(date_val, str):
        try:
            return datetime.strptime(date_val.strip(), '%d/%m/%Y').date()
        except:
            try:
                return datetime.strptime(date_val.strip(), '%Y-%m-%d').date()
            except:
                return None
    return None


def get_day_of_week(date_obj):
    """Get day of week from date"""
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    return days[date_obj.weekday()]


def get_week_of_month(date_obj):
    """
    Get which week of the month this date falls in (1-4+)
    Week starts on Monday
    """
    # Find the first Monday of the month
    first_day = date_obj.replace(day=1)
    first_monday = first_day
    
    # If month doesn't start on Monday, find first Monday
    if first_day.weekday() != 0:  # 0 = Monday
        days_until_monday = (7 - first_day.weekday()) % 7
        if days_until_monday > 0:
            first_monday = first_day + timedelta(days=days_until_monday)
    
    # Calculate which week this date is in
    if date_obj < first_monday:
        # Days before first Monday are "week 0" or belong to previous month's last week
        return 0
    
    days_since_first_monday = (date_obj - first_monday).days
    week_number = (days_since_first_monday // 7) + 1
    
    return week_number


def map_source_date_backward_4_weeks(source_date):
    """
    Map source date to target by subtracting 4 weeks (28 days).
    Accepts result in January or February 2026.
    """
    target_date = source_date - timedelta(days=28)
    if target_date.year == 2026 and target_date.month in [1, 2]:
        return target_date
    return None


def _normalize_type_name(raw):
    """Normalize availability type name for lookup (strip, lowercase). Default to 'Core' if empty."""
    if raw is None:
        return 'core'
    s = str(raw).strip()
    return s.lower() if s else 'core'


def process_xlsx_file(filepath, users_map):
    """Process the availability XLSX file - March 2026 data. Reads type name from column TYPE_NAME_COLUMN_INDEX."""
    logger.info(f"\nProcessing: {filepath}")
    
    try:
        wb = openpyxl.load_workbook(filepath, data_only=True)
    except Exception as e:
        logger.error(f"Failed to load workbook: {e}")
        return {}, []
    
    if 'Care Assistant Availability' not in wb.sheetnames:
        logger.error("'Care Assistant Availability' sheet not found")
        return {}, []
    
    ws = wb['Care Assistant Availability']
    
    # Group records by user
    user_records = defaultdict(list)
    unmatched_users = set()
    
    # Parse rows (skip header)
    for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), 2):
        care_assistant_name = row[CARE_ASSISTANT_NAME_COLUMN]
        start_date_val = row[START_DATE_COLUMN]
        start_time_val = row[START_TIME_COLUMN]
        end_date_val = row[END_DATE_COLUMN]
        end_time_val = row[END_TIME_COLUMN]
        type_name_raw = row[TYPE_NAME_COLUMN_INDEX] if len(row) > TYPE_NAME_COLUMN_INDEX else None
        
        if not care_assistant_name:
            continue
        
        # Strip title and match user
        name_without_title = strip_title(care_assistant_name)
        user_key = name_without_title.strip().lower()
        
        user_id = users_map.get(user_key)
        if not user_id:
            unmatched_users.add(care_assistant_name)
            continue
        
        # Parse dates and times
        start_date = parse_date_value(start_date_val)
        end_date = parse_date_value(end_date_val)
        start_time = parse_time_value(start_time_val)
        end_time = parse_time_value(end_time_val)
        
        if not all([start_date, start_time, end_time]):
            logger.warning(f"Row {row_num}: Missing date/time data")
            continue
        
        # Only process March 2026 data
        if start_date.year != 2026 or start_date.month != 3:
            continue
        
        user_records[user_id].append({
            'march_date': start_date,
            'start_time': start_time,
            'end_time': end_time,
            'end_date': end_date,
            'type_name': _normalize_type_name(type_name_raw),
            'source_file': str(filepath),
            'source_row': row_num,
            'source_sheet': 'Care Assistant Availability',
            'type_name_raw': type_name_raw,
        })
    
    logger.info(f"Extracted records for {len(user_records)} users from March 2026")
    
    return user_records, list(unmatched_users)


def handle_overnight_shift(pattern):
    """Split overnight shifts into two separate availability records"""
    start_time = pattern['start_time']
    end_time = pattern['end_time']
    start_date = pattern['start_date']
    
    if end_time < start_time:
        # Overnight shift - split into two
        patterns = []
        
        # First part: start_time to 23:59:59
        patterns.append({
            'day_of_week': get_day_of_week(start_date),
            'start_time': start_time,
            'end_time': time(23, 59, 59),
            'start_date': start_date,
            'occurs_every': pattern['occurs_every']
        })
        
        # Second part: 00:00:00 to end_time next day
        next_day = start_date + timedelta(days=1)
        patterns.append({
            'day_of_week': get_day_of_week(next_day),
            'start_time': time(0, 0, 0),
            'end_time': end_time,
            'start_date': next_day,
            'occurs_every': pattern['occurs_every']
        })
        
        return patterns
    else:
        # Normal shift - return as is with day_of_week
        return [{
            'day_of_week': get_day_of_week(start_date),
            'start_time': start_time,
            'end_time': end_time,
            'start_date': start_date,
            'occurs_every': pattern['occurs_every']
        }]


def check_if_weekly_pattern(all_records, jan_date, start_time, end_time):
    """
    Check if a day+time combination appears weekly (occurs_every=1) or monthly (occurs_every=4)
    By checking how many times this day-of-week + time appears in February
    """
    if not jan_date:
        return False
    
    day_of_week = jan_date.weekday()
    
    # Count occurrences of this day+time in February
    count = 0
    for record in all_records:
        if (record['feb_date'].weekday() == day_of_week and
            record['start_time'] == start_time and
            record['end_time'] == end_time):
            count += 1
    
    # If appears 4 times (once per week in Feb), it's weekly
    # Otherwise it's monthly
    return count >= 4


def calculate_correct_start_date(jan_date, all_records, start_time, end_time):
    """
    For monthly patterns (occurs_every=4), we need to ensure that all February dates
    for this day+time are exactly 0, 4, 8... weeks from the startDate.
    
    This function finds the correct startDate in January that makes the recurrence work.
    """
    if not jan_date:
        return None
    
    day_of_week = jan_date.weekday()
    
    # Get all February dates for this day+time
    feb_dates = []
    for record in all_records:
        if (record['feb_date'].weekday() == day_of_week and
            record['start_time'] == start_time and
            record['end_time'] == end_time):
            feb_dates.append(record['feb_date'])
    
    if not feb_dates:
        return jan_date
    
    # For each February date, calculate how many weeks it is from jan_date
    weeks_from_jan = []
    for feb_date in feb_dates:
        days_diff = (feb_date - jan_date).days
        weeks = days_diff // 7
        weeks_from_jan.append(weeks)
    
    # Check if all weeks are divisible by 4 (occurs_every=4)
    # If not, we need to adjust the start date
    all_divisible_by_4 = all(w % 4 == 0 for w in weeks_from_jan)
    
    if all_divisible_by_4:
        return jan_date
    
    # If not all divisible, we need to find an earlier date that works
    # Try backing up by 1, 2, or 3 weeks to find a date where all Feb dates are 4-week multiples
    for backup_weeks in range(1, 4):
        test_date = jan_date - timedelta(weeks=backup_weeks)
        
        # Make sure it's still in January
        if test_date.month != 1:
            continue
        
        # Check if all Feb dates are now divisible by 4 from this date
        all_work = True
        for feb_date in feb_dates:
            days_diff = (feb_date - test_date).days
            weeks = days_diff // 7
            if weeks % 4 != 0:
                all_work = False
                break
        
        if all_work:
            return test_date
    
    # If we can't find a better date, return the original
    return jan_date


def _resolve_type(type_name, availability_types_map, source_context=None):
    """Resolve type_id and is_unavailability from type name. Returns (type_id, is_unavailability)."""
    info = availability_types_map.get(type_name)
    if info:
        return info['id'], info['is_unavailability']
    # Log with source location so user can fix column mapping or data (e.g. wrong column = client/area name)
    if source_context:
        logger.warning(
            "Availability type not found in DB (will insert with type_id=NULL): type_name=%r | "
            "source: file=%s, sheet=%s, row=%s, column_index=%s (TYPE_NAME_COLUMN_INDEX), raw_value=%r",
            type_name,
            source_context.get('source_file', '?'),
            source_context.get('source_sheet', '?'),
            source_context.get('source_row', '?'),
            TYPE_NAME_COLUMN_INDEX,
            source_context.get('type_name_raw'),
        )
    else:
        logger.warning("Availability type not found in DB (will insert with type_id=NULL): %r", type_name)
    return None, False


def generate_january_availabilities(user_records_map, availability_types_map):
    """
    Generate January & February 2026 availability records from March 2026 data (4 weeks backward).
    - Core → recurring: is_temp=False, start_date, end_date, occurs_every=4.
    - Other types → temporary: is_temp=True, effective_date_from, effective_date_to.
    - type_id and is_unavailability come from availability_types (by type name).
    """
    availabilities = []
    
    for user_id, records in user_records_map.items():
        logger.info(f"  Processing user_id={user_id} ({len(records)} March records)")
        
        for record in records:
            march_date = record['march_date']
            target_date = map_source_date_backward_4_weeks(march_date)
            if not target_date:
                logger.debug(f"    Skipping {march_date} - mapped date not in Jan/Feb 2026")
                continue
            
            type_name = record.get('type_name') or 'core'
            source_context = {
                'source_file': record.get('source_file'),
                'source_row': record.get('source_row'),
                'source_sheet': record.get('source_sheet'),
                'type_name_raw': record.get('type_name_raw'),
            } if record.get('source_file') is not None else None
            type_id, is_unavailability = _resolve_type(type_name, availability_types_map, source_context)
            is_core = type_name == 'core'
            
            # For temp records: effective dates = Excel dates as-is (no backward mapping)
            excel_end_date = record.get('end_date') or march_date
            
            if is_core:
                # Recurring: is_temp=False, start_date, end_date, occurs_every=4
                shift_patterns = handle_overnight_shift({
                    'start_time': record['start_time'],
                    'end_time': record['end_time'],
                    'start_date': target_date,
                    'occurs_every': 4
                })
                for shift in shift_patterns:
                    if shift['start_date'].year == 2026 and shift['start_date'].month in [1, 2]:
                        availabilities.append({
                            'user_id': user_id,
                            'days': [shift['day_of_week']],
                            'start_time': shift['start_time'],
                            'end_time': shift['end_time'],
                            'is_temp': False,
                            'is_unavailability': is_unavailability,
                            'type_id': type_id,
                            'start_date': shift['start_date'],
                            'end_date': None,
                            'occurs_every': 4,
                            'effective_date_from': None,
                            'effective_date_to': None,
                        })
            else:
                # Temporary: is_temp=True; effective_date_from/to = Excel dates (no 4-week backward mapping)
                shift_patterns = handle_overnight_shift({
                    'start_time': record['start_time'],
                    'end_time': record['end_time'],
                    'start_date': target_date,
                    'occurs_every': 1
                })
                for shift in shift_patterns:
                    if shift['start_date'].year == 2026 and shift['start_date'].month in [1, 2]:
                        availabilities.append({
                            'user_id': user_id,
                            'days': [shift['day_of_week']],
                            'start_time': shift['start_time'],
                            'end_time': shift['end_time'],
                            'is_temp': True,
                            'is_unavailability': is_unavailability,
                            'type_id': type_id,
                            'start_date': None,
                            'end_date': None,
                            'occurs_every': None,
                            'effective_date_from': march_date,
                            'effective_date_to': excel_end_date,
                        })
        
        logger.info(f"    → Generated {len([a for a in availabilities if a['user_id'] == user_id])} records")
    
    return availabilities


def deduplicate_availabilities(availabilities):
    """Remove duplicate availabilities; keep separate records per user/day/time/date/type."""
    unique_map = {}
    for avail in availabilities:
        date_part = avail.get('start_date') or avail.get('effective_date_from')
        key = (
            avail['user_id'],
            tuple(avail['days']),
            avail['start_time'],
            avail['end_time'],
            date_part,
            avail.get('type_id'),
            avail.get('is_temp'),
        )
        unique_map[key] = avail
    return list(unique_map.values())


def seed_availabilities(connection, availabilities):
    """Insert user availabilities into database (with type_id)."""
    if not availabilities:
        logger.warning("No availabilities to insert")
        return False
    
    unique_availabilities = deduplicate_availabilities(availabilities)
    logger.info(f"After deduplication: {len(unique_availabilities)} unique records")
    
    logger.info(f"Inserting {len(unique_availabilities)} availability records...")
    
    cursor = connection.cursor()
    try:
        insert_query = """
            INSERT INTO user_availabilities (
                user_id, days, start_time, end_time, is_temp, is_unavailability, type_id,
                start_date, end_date, occurs_every, effective_date_from, effective_date_to,
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
                avail.get('type_id'),
                avail.get('start_date'),
                avail.get('end_date'),
                avail.get('occurs_every'),
                avail.get('effective_date_from'),
                avail.get('effective_date_to'),
            )
            for avail in unique_availabilities
        ]
        
        execute_values(
            cursor,
            insert_query,
            availability_tuples,
            template="(%s, %s::text[]::user_availabilities_days_enum[], %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"
        )
        
        inserted = cursor.fetchall()
        connection.commit()
        
        logger.info(f"✓ Successfully inserted {len(inserted)} availability records")
        return True
        
    except Exception as e:
        connection.rollback()
        logger.error(f"✗ Failed to insert availabilities: {e}")
        raise
    finally:
        cursor.close()


def run():
    """Main execution function"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║   User Availability Migration - January & February 2026   ║
    ║   From March 2026 data (4 weeks backward); Core=recurring, others=temp ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    config = get_db_config()
    
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in .env file")
        return False
    
    xlsx_path = Path(__file__).parent.parent / 'assets' / 'userAvailabilities' / 'userAvailabilities.xlsx'
    
    if not xlsx_path.exists():
        logger.error(f"File not found: {xlsx_path}")
        return False
    
    connection = None
    try:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: DATABASE CONNECTION")
        logger.info("="*60)
        connection = connect_to_database(config)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 2: LOAD USERS & AVAILABILITY TYPES")
        logger.info("="*60)
        users_map = get_all_users(connection)
        availability_types_map = get_availability_types(connection)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 3: EXTRACT MARCH 2026 DATA")
        logger.info("="*60)
        user_records_map, unmatched_users = process_xlsx_file(xlsx_path, users_map)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 4: GENERATE JANUARY & FEBRUARY 2026 AVAILABILITIES")
        logger.info("="*60)
        availabilities = generate_january_availabilities(user_records_map, availability_types_map)
        logger.info(f"Generated {len(availabilities)} January & February 2026 availability records")
        
        logger.info("\n" + "="*60)
        logger.info("STEP 5: SEED DATABASE")
        logger.info("="*60)
        
        if availabilities:
            success = seed_availabilities(connection, availabilities)
        else:
            logger.warning("No availabilities generated")
            success = False
        
        if unmatched_users:
            logger.warning("\n" + "="*60)
            logger.warning("UNMATCHED USERS")
            logger.warning("="*60)
            logger.warning(f"The following {len(unmatched_users)} users were not found in database:")
            for user_name in sorted(unmatched_users):
                logger.warning(f"  - {user_name}")
        
        if success:
            print("\n" + "="*60)
            print("✓ JANUARY & FEBRUARY 2026 AVAILABILITY MIGRATION COMPLETED")
            print("="*60)
            return True
        else:
            print("\n" + "="*60)
            print("✗ JANUARY & FEBRUARY 2026 AVAILABILITY MIGRATION FAILED")
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