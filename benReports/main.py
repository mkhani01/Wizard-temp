#!/usr/bin/env python3
"""
Client Schedule Comparison Script (Simplified)
Compares client availability schedules between PostgreSQL database and Excel file.
Compares only day-of-week and time patterns, ignoring dates and recurrence.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple
import csv
import sys
import os

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 6969,
    'database': 'appDB',
    'user': 'root',
    'password': 'root',
    'connect_timeout': 10,
}

# Excel file configuration
EXCEL_FILE = './clientHours.xlsx'

# Day mapping
DAY_MAPPING = {
    0: 'Monday',
    1: 'Tuesday',
    2: 'Wednesday',
    3: 'Thursday',
    4: 'Friday',
    5: 'Saturday',
    6: 'Sunday'
}

# Global table names (will be discovered)
TABLE_NAMES = {}

def connect_to_db():
    """Establish database connection."""
    try:
        print(f"Connecting to: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✓ Connected to database successfully")
        return conn
    except Exception as e:
        print(f"✗ Database connection error: {e}")
        print(f"\nPlease verify:")
        print(f"  - PostgreSQL is running on port {DB_CONFIG['port']}")
        print(f"  - Database '{DB_CONFIG['database']}' exists")
        print(f"  - User '{DB_CONFIG['user']}' has access")
        raise

def discover_table_names(conn):
    """Discover actual table names in the database."""
    global TABLE_NAMES
    
    # First, try to get current database and schema
    with conn.cursor() as cur:
        cur.execute("SELECT current_database(), current_schema()")
        db_name, schema_name = cur.fetchone()
        print(f"Current database: {db_name}, schema: {schema_name}")
    
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (schema_name,))
        tables = [row[0] for row in cur.fetchall()]
    
    print(f"\n📋 Found {len(tables)} tables in schema '{schema_name}'")
    
    if len(tables) == 0:
        print(f"\n✗ ERROR: No tables found in schema '{schema_name}'")
        raise Exception(f"No tables found in schema '{schema_name}'")
    
    # Show all tables with client/availability in name
    relevant_tables = [t for t in tables if 'client' in t.lower() or 'availability' in t.lower()]
    
    if relevant_tables:
        print(f"\nRelevant tables:")
        for table in relevant_tables:
            print(f"  - {table}")
    
    # Map logical names to actual table names
    table_mapping = {
        'client': ['client', 'Client'],
        'client_availabilities': ['client_availabilities', 'client_availability', 'clientAvailabilities', 'ClientAvailabilities'],
    }
    
    for logical_name, possible_names in table_mapping.items():
        found = False
        for name in possible_names:
            if name in tables:
                # Check if needs quotes (mixed case)
                if name != name.lower():
                    TABLE_NAMES[logical_name] = f'"{name}"'
                else:
                    TABLE_NAMES[logical_name] = name
                found = True
                break
        
        if not found:
            print(f"\n✗ ERROR: Could not find table for '{logical_name}'")
            print(f"  Searched for: {possible_names}")
            if relevant_tables:
                print(f"  Available client tables: {relevant_tables}")
            raise Exception(f"Could not find table for '{logical_name}'. Checked: {possible_names}")
    
    print(f"\n✓ Discovered table mappings:")
    for logical, actual in TABLE_NAMES.items():
        print(f"  {logical} → {actual}")
    print()

def normalize_name(name: str) -> str:
    """Normalize name by trimming spaces and converting to lowercase."""
    return name.strip().lower() if name else ""

def parse_service_location_name(service_location_name: str) -> Tuple[str, str]:
    """
    Parse 'Service Location Name' in format 'Lastname, Firstname'.
    Returns (firstname, lastname) both normalized.
    """
    if not service_location_name or ',' not in service_location_name:
        return ("", "")
    
    parts = service_location_name.split(',', 1)
    lastname = normalize_name(parts[0])
    firstname = normalize_name(parts[1])
    return (firstname, lastname)

def get_clients_from_db(conn) -> Dict[int, Dict]:
    """Fetch all clients from database."""
    query = f"""
        SELECT 
            id,
            name as firstname,
            lastname,
            status,
            deleted_at
        FROM {TABLE_NAMES['client']}
        WHERE deleted_at IS NULL
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        clients = cur.fetchall()
    
    # Create lookup dictionary by normalized name
    clients_dict = {}
    clients_by_name = {}
    
    for client in clients:
        client_id = client['id']
        firstname = normalize_name(client['firstname'])
        lastname = normalize_name(client['lastname'])
        
        clients_dict[client_id] = {
            'id': client_id,
            'firstname': client['firstname'],
            'lastname': client['lastname'],
            'firstname_norm': firstname,
            'lastname_norm': lastname,
            'status': client['status']
        }
        
        # Create name lookup key
        name_key = f"{firstname}|{lastname}"
        clients_by_name[name_key] = client_id
    
    print(f"✓ Found {len(clients_dict)} active clients in database")
    return clients_dict, clients_by_name

def get_client_availabilities(conn, client_id: int) -> List[Dict]:
    """Fetch all availabilities for a specific client."""
    query = f"""
        SELECT 
            id,
            client_id,
            days,
            requested_start_time,
            requested_end_time,
            is_unavailability
        FROM {TABLE_NAMES['client_availabilities']}
        WHERE client_id = %s
        AND deleted_at IS NULL
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (client_id,))
        results = cur.fetchall()
    
    # Convert to dict with consistent keys
    availabilities = []
    for row in results:
        # PostgreSQL returns array as Python list
        days_array = row['days']
        
        # Handle different formats of days
        if isinstance(days_array, list):
            # Already a list - use as is
            days_list = days_array
        elif isinstance(days_array, str):
            # String format like "{Friday}" or "Friday"
            days_str = days_array.strip('{}')
            days_list = [d.strip() for d in days_str.split(',') if d.strip()]
        else:
            days_list = []
        
        availabilities.append({
            'id': row['id'],
            'client_id': row['client_id'],
            'days': days_list,
            'requestedStartTime': row['requested_start_time'],
            'requestedEndTime': row['requested_end_time'],
            'isUnavailability': row['is_unavailability'],
        })
    
    return availabilities

def get_all_db_schedules(conn, clients_dict: Dict) -> Dict:
    """
    Get all schedules from database as day/time patterns.
    Returns dict: {client_id: [patterns]} where each pattern is {day, start_time, end_time}
    """
    all_schedules = {}
    processed = 0
    total = len(clients_dict)
    debug_client_count = 0
    
    for client_id in clients_dict.keys():
        availabilities = get_client_availabilities(conn, client_id)
        client_patterns = []
        
        for avail in availabilities:
            if avail['isUnavailability']:
                continue  # Skip unavailability records
            
            # Debug: Show first few clients' patterns
            if debug_client_count < 3 and avail['days']:
                client_name = f"{clients_dict[client_id]['firstname']} {clients_dict[client_id]['lastname']}"
                print(f"  Debug - {client_name}: days={avail['days']}, time={avail['requestedStartTime']}-{avail['requestedEndTime']}")
            
            # Extract day/time patterns from availability
            for day in avail['days']:
                client_patterns.append({
                    'day': day,
                    'start_time': str(avail['requestedStartTime']),
                    'end_time': str(avail['requestedEndTime']),
                    'availability_id': avail['id']
                })
        
        if client_patterns:
            all_schedules[client_id] = client_patterns
            debug_client_count += 1
        
        processed += 1
        if processed % 100 == 0:
            print(f"  Processed {processed}/{total} clients...")
    
    print(f"✓ Loaded schedules for {len(all_schedules)} clients")
    return all_schedules

def parse_excel_datetime(dt_value) -> Tuple[datetime, str, str]:
    """
    Parse Excel datetime and return (datetime, time_str, day_name).
    """
    if pd.isna(dt_value) or not dt_value:
        return None, None, None
    
    try:
        # If it's already a datetime object
        if isinstance(dt_value, datetime):
            dt = dt_value
        elif isinstance(dt_value, pd.Timestamp):
            dt = dt_value.to_pydatetime()
        else:
            # Try to parse as string
            dt = pd.to_datetime(dt_value)
        
        time_str = dt.strftime('%H:%M:%S')
        day_name = DAY_MAPPING[dt.weekday()]
        return dt, time_str, day_name
    except Exception as e:
        return None, None, None

def load_excel_file(excel_file: str) -> pd.DataFrame:
    """Load Excel file from 'Data' sheet, trying different header rows."""
    print(f"Loading Excel file: {excel_file}")
    
    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Excel file not found: {excel_file}")
    
    # Read from 'Data' sheet
    sheet_name = 'Data'
    print(f"Reading from sheet: '{sheet_name}'")
    
    # Try different header rows
    for header_row in [0, 1, 2]:
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=header_row)
            if 'Service Location Name' in df.columns:
                print(f"✓ Found correct header at row {header_row}")
                print(f"✓ Loaded {len(df)} rows from sheet '{sheet_name}'")
                return df
        except Exception as e:
            if header_row == 0:
                print(f"⚠ Warning reading sheet '{sheet_name}': {e}")
            continue
    
    # If no valid header found, show what we have
    try:
        df_preview = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, nrows=5)
        print(f"\nFirst 5 rows of '{sheet_name}' sheet:")
        print(df_preview)
    except Exception as e:
        print(f"\n✗ ERROR: Could not read '{sheet_name}' sheet: {e}")
        
        # Try to list available sheets
        try:
            xl_file = pd.ExcelFile(excel_file)
            print(f"\nAvailable sheets in Excel file:")
            for sheet in xl_file.sheet_names:
                print(f"  - {sheet}")
        except:
            pass
    
    raise ValueError(f"Could not find 'Service Location Name' column in '{sheet_name}' sheet. Please check Excel file format.")

def load_excel_schedules(excel_file: str, clients_by_name: Dict) -> Tuple[Dict, Set, List]:
    """
    Load schedules from Excel file as day/time patterns.
    Returns (excel_schedules, excel_clients, unmatched_clients)
    """
    df = load_excel_file(excel_file)
    
    excel_schedules = {}
    excel_clients = set()
    unmatched_clients = []
    debug_rows = 0
    
    for idx, row in df.iterrows():
        service_location_name = row.get('Service Location Name', '')
        
        if pd.isna(service_location_name) or not service_location_name:
            continue
        
        # Parse name
        firstname, lastname = parse_service_location_name(service_location_name)
        
        if not firstname or not lastname:
            continue
        
        name_key = f"{firstname}|{lastname}"
        excel_clients.add(name_key)
        
        # Try to find client in database
        client_id = clients_by_name.get(name_key)
        
        if not client_id:
            unmatched_clients.append({
                'service_location_name': service_location_name,
                'parsed_firstname': firstname,
                'parsed_lastname': lastname
            })
            continue
        
        # Parse start and end datetime
        start_dt, start_time, day_name = parse_excel_datetime(
            row.get('Service Requirement Start Date And Time')
        )
        end_dt, end_time, _ = parse_excel_datetime(
            row.get('Service Requirement End Date And Time')
        )
        
        if not start_dt or not end_dt or not day_name:
            continue
        
        # Debug: Show first few Excel patterns
        if debug_rows < 5:
            print(f"  Debug Excel - {service_location_name}: day={day_name}, time={start_time}-{end_time}")
            debug_rows += 1
        
        # Store as day/time pattern
        if client_id not in excel_schedules:
            excel_schedules[client_id] = []
        
        excel_schedules[client_id].append({
            'day': day_name,
            'start_time': start_time,
            'end_time': end_time,
            'row_number': idx + 2  # Excel row number (1-indexed + header)
        })
    
    print(f"✓ Found {len(excel_clients)} unique clients in Excel")
    print(f"✓ Matched {len(excel_schedules)} clients with database")
    
    if unmatched_clients:
        print(f"⚠ {len(unmatched_clients)} clients in Excel not found in database")
    
    return excel_schedules, excel_clients, unmatched_clients

def compare_schedules(db_schedules: Dict, excel_schedules: Dict, 
                     clients_dict: Dict) -> List[Dict]:
    """
    Compare database and Excel schedules by day/time patterns.
    Returns list of differences.
    """
    differences = []
    
    # Get all client IDs that have schedules in either source
    all_client_ids = set(db_schedules.keys()) | set(excel_schedules.keys())
    
    for client_id in all_client_ids:
        client = clients_dict.get(client_id, {})
        client_name = f"{client.get('firstname', '')} {client.get('lastname', '')}"
        
        db_patterns = db_schedules.get(client_id, [])
        excel_patterns = excel_schedules.get(client_id, [])
        
        # Group patterns by day
        db_by_day = {}
        for pattern in db_patterns:
            day = pattern['day']
            if day not in db_by_day:
                db_by_day[day] = []
            db_by_day[day].append(pattern)
        
        excel_by_day = {}
        for pattern in excel_patterns:
            day = pattern['day']
            if day not in excel_by_day:
                excel_by_day[day] = []
            excel_by_day[day].append(pattern)
        
        # Compare all days
        all_days = set(db_by_day.keys()) | set(excel_by_day.keys())
        
        for day in all_days:
            db_day_patterns = db_by_day.get(day, [])
            excel_day_patterns = excel_by_day.get(day, [])
            
            # Try to match patterns by time
            matched_db = set()
            matched_excel = set()
            
            for db_idx, db_pat in enumerate(db_day_patterns):
                for excel_idx, excel_pat in enumerate(excel_day_patterns):
                    if (db_pat['start_time'] == excel_pat['start_time'] and
                        db_pat['end_time'] == excel_pat['end_time']):
                        matched_db.add(db_idx)
                        matched_excel.add(excel_idx)
            
            # Report unmatched DB patterns
            for db_idx, db_pat in enumerate(db_day_patterns):
                if db_idx not in matched_db:
                    # Check if there's any Excel pattern for this day
                    if excel_day_patterns:
                        # Mismatch in time
                        differences.append({
                            'client_id': client_id,
                            'client_name': client_name,
                            'day': day,
                            'type': 'TIME_MISMATCH',
                            'db_start_time': db_pat['start_time'],
                            'db_end_time': db_pat['end_time'],
                            'excel_start_time': ', '.join([e['start_time'] for e in excel_day_patterns]),
                            'excel_end_time': ', '.join([e['end_time'] for e in excel_day_patterns]),
                            'availability_id': db_pat['availability_id']
                        })
                    else:
                        # In DB but not in Excel
                        differences.append({
                            'client_id': client_id,
                            'client_name': client_name,
                            'day': day,
                            'type': 'IN_DB_NOT_IN_EXCEL',
                            'db_start_time': db_pat['start_time'],
                            'db_end_time': db_pat['end_time'],
                            'excel_start_time': '',
                            'excel_end_time': '',
                            'availability_id': db_pat['availability_id']
                        })
            
            # Report unmatched Excel patterns
            for excel_idx, excel_pat in enumerate(excel_day_patterns):
                if excel_idx not in matched_excel:
                    if not db_day_patterns:
                        # In Excel but not in DB
                        differences.append({
                            'client_id': client_id,
                            'client_name': client_name,
                            'day': day,
                            'type': 'IN_EXCEL_NOT_IN_DB',
                            'db_start_time': '',
                            'db_end_time': '',
                            'excel_start_time': excel_pat['start_time'],
                            'excel_end_time': excel_pat['end_time'],
                            'availability_id': '',
                            'excel_row': excel_pat.get('row_number', '')
                        })
    
    print(f"✓ Found {len(differences)} schedule differences")
    return differences

def find_clients_only_in_db(clients_dict: Dict, excel_clients: Set) -> List[Dict]:
    """Find clients that exist in DB but not in Excel."""
    only_in_db = []
    
    for client_id, client in clients_dict.items():
        name_key = f"{client['firstname_norm']}|{client['lastname_norm']}"
        if name_key not in excel_clients:
            only_in_db.append({
                'client_id': client_id,
                'firstname': client['firstname'],
                'lastname': client['lastname'],
                'status': client['status']
            })
    
    print(f"✓ Found {len(only_in_db)} clients only in database")
    return only_in_db

def write_differences_report(differences: List[Dict], filename: str):
    """Write schedule differences to CSV."""
    if not differences:
        print(f"⚠ No differences found")
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['message'])
            writer.writerow(['No schedule differences found'])
        return
    
    fieldnames = [
        'client_id', 'client_name', 'day', 'type',
        'db_start_time', 'db_end_time', 'excel_start_time', 'excel_end_time',
        'availability_id', 'excel_row'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for diff in differences:
            writer.writerow({
                'client_id': diff.get('client_id', ''),
                'client_name': diff.get('client_name', ''),
                'day': diff.get('day', ''),
                'type': diff.get('type', ''),
                'db_start_time': diff.get('db_start_time', ''),
                'db_end_time': diff.get('db_end_time', ''),
                'excel_start_time': diff.get('excel_start_time', ''),
                'excel_end_time': diff.get('excel_end_time', ''),
                'availability_id': diff.get('availability_id', ''),
                'excel_row': diff.get('excel_row', '')
            })
    
    print(f"✓ Wrote differences report to {filename}")

def write_clients_only_in_db(clients: List[Dict], filename: str):
    """Write clients only in DB to CSV."""
    if not clients:
        print(f"⚠ No clients only in DB")
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['message'])
            writer.writerow(['No clients found only in database'])
        return
    
    fieldnames = ['client_id', 'firstname', 'lastname', 'status']
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clients)
    
    print(f"✓ Wrote clients only in DB to {filename}")

def write_clients_only_in_excel(clients: List[Dict], filename: str):
    """Write clients only in Excel to CSV."""
    if not clients:
        print(f"⚠ No unmatched clients in Excel")
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['message'])
            writer.writerow(['All Excel clients found in database'])
        return
    
    fieldnames = ['service_location_name', 'parsed_firstname', 'parsed_lastname']
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clients)
    
    print(f"✓ Wrote clients only in Excel to {filename}")

def main():
    """Main execution function."""
    print("=" * 70)
    print("CLIENT SCHEDULE COMPARISON TOOL (SIMPLIFIED)")
    print("Compares day-of-week and time patterns only")
    print("=" * 70)
    print()
    
    # Check if Excel file exists
    if not os.path.exists(EXCEL_FILE):
        print(f"✗ ERROR: Excel file not found: {EXCEL_FILE}")
        print(f"\nPlease ensure the file exists in the current directory.")
        return False
    
    # Connect to database
    print("🔌 Connecting to database...")
    conn = None
    try:
        conn = connect_to_db()
        print()
        
        # Discover table names
        print("🔍 Discovering database schema...")
        discover_table_names(conn)
        
        # Load clients from database
        print("📊 Loading data from database...")
        clients_dict, clients_by_name = get_clients_from_db(conn)
        print()
        
        # Get DB schedules as day/time patterns
        print("📅 Loading client availability patterns...")
        db_schedules = get_all_db_schedules(conn, clients_dict)
        print()
        
        # Load Excel schedules
        print("📋 Loading Excel file...")
        excel_schedules, excel_clients, unmatched_excel_clients = load_excel_schedules(
            EXCEL_FILE, clients_by_name
        )
        print()
        
        # Compare schedules
        print("🔍 Comparing day/time patterns...")
        differences = compare_schedules(db_schedules, excel_schedules, clients_dict)
        print()
        
        # Find clients only in DB
        print("👥 Finding clients only in database...")
        clients_only_db = find_clients_only_in_db(clients_dict, excel_clients)
        print()
        
        # Generate reports
        print("📝 Generating reports...")
        write_differences_report(differences, 'report_1_schedule_differences.csv')
        write_clients_only_in_db(clients_only_db, 'report_2_clients_only_in_db.csv')
        write_clients_only_in_excel(unmatched_excel_clients, 'report_3_clients_only_in_excel.csv')
        print()
        
        # Summary
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Schedule differences found: {len(differences)}")
        print(f"Clients only in database: {len(clients_only_db)}")
        print(f"Clients only in Excel: {len(unmatched_excel_clients)}")
        print()
        print("✓ All reports generated successfully!")
        print()
        print("Output files:")
        print("  1. report_1_schedule_differences.csv - Day/time pattern differences")
        print("  2. report_2_clients_only_in_db.csv - Clients in DB but not in Excel")
        print("  3. report_3_clients_only_in_excel.csv - Clients in Excel but not in DB")
        print("=" * 70)
        return True
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if conn:
            conn.close()
            print("\n🔌 Database connection closed")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)