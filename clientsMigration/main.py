"""
Clients Migration
Seeds client table from CSV
"""

import os
import csv
import logging
from pathlib import Path
from datetime import datetime
import psycopg2

from migration_support import get_assets_dir
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
        
        # Get client groups
        cursor.execute("SELECT id, name FROM clients_group")
        lookups['groups'] = {row['name']: row['id'] for row in cursor.fetchall()}
        logger.info(f"Loaded {len(lookups['groups'])} client groups")
        
        # Get areas (for area_id) - table may be "area"
        try:
            cursor.execute("SELECT id, name FROM area")
            lookups['areas'] = {row['name']: row['id'] for row in cursor.fetchall()}
            logger.info(f"Loaded {len(lookups['areas'])} areas")
        except Exception:
            lookups['areas'] = {}
            logger.info("Area table not found or empty; area_id will be null")
        
        return lookups
        
    finally:
        cursor.close()


def safe_strip(value):
    """Safely strip a value that might be None"""
    if value is None:
        return ''
    return str(value).strip()


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


def parse_datetime(date_str):
    """Parse datetime from CSV format DD/MM/YYYY HH:MM:SS"""
    if not date_str:
        return None
    
    date_str = safe_strip(date_str)
    if not date_str:
        return None
    
    try:
        # Format: 01/01/2024 13:49:14
        return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
    except:
        try:
            # Try without time
            return datetime.strptime(date_str, '%d/%m/%Y')
        except:
            logger.warning(f"Could not parse datetime: {date_str}")
            return None


def parse_date(date_str):
    """Parse date from CSV format DD/MM/YYYY HH:MM:SS"""
    dt = parse_datetime(date_str)
    return dt.date() if dt else None


def map_gender(csv_gender):
    """Map CSV gender to entity enum"""
    gender_map = {
        'Male': 'Male',
        'Female': 'Female',
        'Other': 'Other',
        'Prefer Not to Say': 'Prefer not to say',
        'Prefer not to say': 'Prefer not to say',
    }
    return gender_map.get(csv_gender, None)


def map_status(csv_status):
    """Map CSV status to entity enum"""
    status_map = {
        'Active': 'Active',
        'Deactive': 'Deactive',
        'Pending': 'Pending',
        'Uncomplete': 'Uncomplete',
    }
    return status_map.get(csv_status, 'Uncomplete')


def map_service_priority(csv_priority):
    """Map CSV service priority to entity enum (ServicePriority)"""
    if not csv_priority:
        return None
    priority_map = {
        'Very High': 'Very High',
        'High': 'High',
        'Medium': 'Medium',
        'Low': 'Low',
    }
    return priority_map.get(csv_priority.strip(), None)


def map_consent_status(csv_value):
    """Map CSV Consent Status to entity enum (ConsentStatus: disclosed, none, undisclosed)"""
    if not csv_value:
        return None
    v = csv_value.strip().lower()
    if v in ('disclosed', 'Disclosed'):
        return 'disclosed'
    if v in ('undisclosed', 'Undisclosed'):
        return 'undisclosed'
    if v in ('none', 'None', ''):
        return 'none'
    return 'none'


def map_living_circumstances(csv_value):
    """Map CSV Living Circumstances to entity enum (alone, family, nursing home)"""
    if not csv_value:
        return None
    v = csv_value.strip().lower()
    if 'alone' in v or v == 'alone':
        return 'alone'
    if 'family' in v or v == 'family':
        return 'family'
    if 'nursing' in v or 'nursing home' in v or v == 'nursing home':
        return 'nursing home'
    return None


def parse_cognitive_status(row, safe_get_fn):
    """Infer CognitiveStatus from custom boolean columns (Normal, Impaired, Dementia)"""
    def truthy(val):
        s = safe_get_fn(val)
        if not s:
            return False
        return str(s).strip().lower() in ('true', '1', 'yes', 'y')
    # CSV has: MentalCapacity, LearningDisability, MentalHealth, MultiDiagnosis, ComplexNeeds, etc.
    if truthy('ServiceLocationCustom_LearningDisability') or truthy('ServiceLocationCustom_MentalHealth') or truthy('ServiceLocationCustom_MultiDiagnosis') or truthy('ServiceLocationCustom_ComplexNeeds') or truthy('ServiceLocationCustom_DualDiagnosis'):
        return 'Impaired'
    if truthy('ServiceLocationCustom_MentalCapacity'):  # sometimes used as "has capacity" = Normal
        return 'Normal'
    return None


def parse_boolean_csv(value):
    """Parse CSV boolean: True, False, 1, 0, Yes, No"""
    if value is None:
        return False
    s = str(value).strip().lower()
    if s in ('true', '1', 'yes', 'y'):
        return True
    if s in ('false', '0', 'no', 'n'):
        return False
    return False


def seed_client_groups_from_csv(connection, csv_path):
    """Extract and seed unique client groups from CSV Area column"""
    groups = set()
    
    logger.info(f"Reading CSV for client groups: {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # FIX: Handle None values properly
            area = safe_strip(row.get('Area'))
            if area:
                groups.add(area)
    
    logger.info(f"Found {len(groups)} unique client groups:")
    for group in sorted(groups):
        logger.info(f"  - {group}")
    
    if not groups:
        logger.warning("No client groups found in CSV")
        return
    
    cursor = connection.cursor()
    try:
        # Check existing groups
        cursor.execute("SELECT id, name FROM clients_group")
        existing = {row['name']: row['id'] for row in cursor.fetchall()}
        
        # Find new groups
        new_groups = groups - set(existing.keys())
        
        if not new_groups:
            logger.info("✓ All client groups already exist")
            return
        
        logger.info(f"Inserting {len(new_groups)} new client groups...")
        
        # Prepare data
        group_data = [
            (group, f"Client group {group}")
            for group in sorted(new_groups)
        ]
        
        # Insert
        insert_query = """
            INSERT INTO clients_group (name, description, created_date, last_modified_date)
            VALUES %s
            RETURNING id, name
        """
        
        execute_values(
            cursor,
            insert_query,
            group_data,
            template="(%s, %s, NOW(), NOW())"
        )
        
        inserted = cursor.fetchall()
        connection.commit()
        
        logger.info(f"✓ Successfully inserted {len(inserted)} client groups:")
        for row in inserted:
            logger.info(f"  - {row['name']} (ID: {row['id']})")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"✗ Failed to insert client groups: {e}")
        raise
    finally:
        cursor.close()


def seed_areas_from_csv(connection, csv_path):
    """
    Extract unique Area values from CSV and insert missing ones into area table.
    Matches Area entity: id, name, size, lat, long, description, created_date, last_modified_date.
    We set only name and description; size, lat, long stay NULL.
    After this, get_lookup_tables will include new areas so client.area_id can be set.
    """
    areas = set()
    logger.info(f"Reading CSV for areas: {csv_path}")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            area = safe_strip(row.get('Area'))
            if area:
                areas.add(area)
    if not areas:
        logger.warning("No areas found in CSV")
        return
    logger.info(f"Found {len(areas)} unique areas")
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id, name FROM area")
        existing = {row['name']: row['id'] for row in cursor.fetchall()}
    except Exception as e:
        logger.warning("Area table not found or not readable; skipping area seeding: %s", e)
        cursor.close()
        return
    new_areas = areas - set(existing.keys())
    if not new_areas:
        logger.info("✓ All areas already exist")
        cursor.close()
        return
    logger.info(f"Inserting {len(new_areas)} new areas...")
    try:
        # Area entity: name (required), description/size/lat/long (nullable), created_date, last_modified_date
        area_data = [(name, f"Area: {name}") for name in sorted(new_areas)]
        insert_query = """
            INSERT INTO area (name, description, created_date, last_modified_date)
            VALUES %s
            RETURNING id, name
        """
        execute_values(
            cursor,
            insert_query,
            area_data,
            template="(%s, %s, NOW(), NOW())"
        )
        inserted = cursor.fetchall()
        connection.commit()
        logger.info(f"✓ Inserted {len(inserted)} areas:")
        for row in inserted:
            logger.info(f"  - {row['name']} (ID: {row['id']})")
    except Exception as e:
        connection.rollback()
        logger.error("✗ Failed to insert areas: %s", e)
        raise
    finally:
        cursor.close()


def extract_clients_from_csv(csv_path, lookups):
    """Extract client data from CSV and map to database fields"""
    clients = []
    
    logger.info(f"Reading CSV: {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row_num = 0
        
        for row in reader:
            row_num += 1
            
            # Helper function for safe CSV field access
            def safe_get(field_name):
                """Safely get and strip CSV field"""
                return safe_strip(row.get(field_name))
            
            # Basic fields
            first_name = safe_get('First Name')
            last_name = safe_get('Last Name')
            
            if not first_name or not last_name:
                logger.warning(f"Row {row_num}: Skipping - missing name")
                continue
            
            # Generate unique identifier for this client
            pin_number = clean_excel_value(safe_get('PIN Number'))
            
            # Map foreign keys
            title_id = None
            title_name = safe_get('Title')
            if title_name and title_name in lookups['titles']:
                title_id = lookups['titles'][title_name]
            
            nationality_id = None
            nationality_name = safe_get('Nationality')
            if nationality_name and nationality_name in lookups['nationalities']:
                nationality_id = lookups['nationalities'][nationality_name]
            
            religion_id = None
            religion_name = safe_get('Religion')
            if religion_name and religion_name in lookups['religions']:
                religion_id = lookups['religions'][religion_name]
            
            origin_id = None
            origin_name = safe_get('Ethnic Origin')
            if origin_name and origin_name in lookups['origins']:
                origin_id = lookups['origins'][origin_name]
            
            # Get group_id for many-to-many relationship (Area column = team/group name)
            group_id = None
            group_name = safe_get('Area')
            if group_name and lookups.get('groups') and group_name in lookups['groups']:
                group_id = lookups['groups'][group_name]
            
            # Area entity (optional; same or different from group)
            area_id = None
            if lookups.get('areas') and group_name and group_name in lookups['areas']:
                area_id = lookups['areas'][group_name]
            
            # Parse dates
            birth_date = parse_date(safe_get('Date Of Birth'))
            start_date = parse_date(safe_get('Start Date'))
            termination_date = parse_date(safe_get('Termination Date'))
            consent_date = parse_date(safe_get('Consent Date'))
            created_date = parse_datetime(safe_get('Service Location Created Date & Time'))
            updated_date = parse_datetime(safe_get('Service Location Updated Date & Time'))
            
            # Map enums
            gender = map_gender(safe_get('Gender'))
            status = map_status(safe_get('Service Location Status'))
            service_priority = map_service_priority(safe_get('Service Location Service Priority'))
            consent_status = map_consent_status(safe_get('Consent Status'))
            living_circumstances = map_living_circumstances(safe_get('ServiceLocationCustom_Living_Circumstances'))
            cognitive_status = parse_cognitive_status(row, safe_get)
            
            # Address lines (include County as extra line if present)
            address_lines = []
            for field in ['Address Line 1', 'Address Line 2', 'Address Line 3']:
                addr = safe_get(field)
                if addr:
                    address_lines.append(addr)
            county = safe_get('County')
            if county:
                address_lines.append(county)
            
            # Post code: clean Excel formulas and extra spaces
            postcode_raw = clean_excel_value(safe_get('Post Code'))
            postcode = postcode_raw if postcode_raw else None
            
            # Booleans from custom columns (entity defaults false)
            disability = parse_boolean_csv(row.get('ServiceLocationCustom_PhysicalDisability'))
            palliative_care = parse_boolean_csv(row.get('ServiceLocationCustom_POA'))  # or DNR etc.
            incontinency = False  # no CSV column
            exercise_need = False
            dysphagia_need = False
            race_sensitivity = False
            language_sensitivity = False
            continuity_required = False
            only_preferred = False
            
            # Build client data (all entity fields we can map from CSV)
            client_data = {
                'name': first_name,
                'lastname': last_name,
                'middle_name': safe_get('Initial') or None,
                'initial': safe_get('Initial') or None,
                'description': safe_get('Run Description') or None,
                'email': safe_get('Email') or None,
                'phone_number': clean_excel_value(safe_get('Phone')) or None,
                'mobile': clean_excel_value(safe_get('Mobile')) or None,
                'website': safe_get('WebSite') or None,
                'termination_date': termination_date,
                'company_name': safe_get('Company Name') or None,
                'preferred_name': safe_get('Preferred Name') or None,
                'hce_number': clean_excel_value(safe_get('NHS Number')) or None,
                'key_safe_number': safe_get('Key Safe Number') or None,
                'access_details': safe_get('Access Details') or None,
                'consent_status': consent_status,
                'consent_date': consent_date,
                'consent_notes': safe_get('Consent Notes') or None,
                'status': status,
                'gender': gender,
                'service_priority': service_priority,
                'start_date': start_date,
                'birth_date': birth_date,
                'address_lines': address_lines if address_lines else None,
                'town': safe_get('City / Town') or None,
                'postcode': postcode,
                'disability': disability,
                'palliative_care': palliative_care,
                'cognitive_status': cognitive_status,
                'incontinency': incontinency,
                'exercise_need': exercise_need,
                'dysphagia_need': dysphagia_need,
                'living_circumstances': living_circumstances,
                'race_sensitivity': race_sensitivity,
                'language_sensitivity': language_sensitivity,
                'continuity_required': continuity_required,
                'only_preferred': only_preferred,
                'title_id': title_id,
                'nationality_id': nationality_id,
                'religion_id': religion_id,
                'origin_id': origin_id,
                'area_id': area_id,
                'group_id': group_id,
                'created_date': created_date or datetime.now(),
                'updated_date': updated_date or datetime.now(),
            }
            
            clients.append(client_data)
    
    logger.info(f"Extracted {len(clients)} clients from CSV")
    return clients


def seed_clients(connection, clients):
    """Insert or update clients using manual upsert (no ON CONFLICT since no unique constraint on name+lastname)"""
    if not clients:
        logger.warning("No clients to insert")
        return False
    
    logger.info(f"Inserting/Updating {len(clients)} clients...")
    
    cursor = connection.cursor()
    try:
        # Load existing clients by (name, lastname)
        cursor.execute("SELECT id, name, lastname FROM client WHERE deleted_at IS NULL")
        existing = {}
        for row in cursor.fetchall():
            key = f"{row['name']}|{row['lastname']}"
            existing[key] = row['id']
        
        logger.info(f"Found {len(existing)} existing clients in database")
        
        # Separate into inserts and updates
        to_insert = []
        to_update = []
        
        for client in clients:
            key = f"{client['name']}|{client['lastname']}"
            if key in existing:
                to_update.append((client, existing[key]))
            else:
                to_insert.append(client)
        
        logger.info(f"  - {len(to_insert)} new clients to insert")
        logger.info(f"  - {len(to_update)} existing clients to update")
        
        # Clients in DB but not in file(s) -> set status to Deactive
        keys_in_file = {f"{c['name']}|{c['lastname']}" for c in clients}
        to_deactivate_ids = [existing[key] for key in existing if key not in keys_in_file]
        if to_deactivate_ids:
            logger.info(f"  - {len(to_deactivate_ids)} existing clients not in file(s) will be deactivated")
        
        processed = []
        
        # INSERT new clients
        if to_insert:
            insert_query = """
                INSERT INTO client (
                    name, lastname, middle_name, description, email, phone_number, mobile, website,
                    termination_date, company_name, initial, preferred_name, hce_number, key_safe_number,
                    access_details, consent_status, consent_date, consent_notes, status, gender,
                    service_priority, start_date, birth_date, address_lines, town, postcode,
                    disability, palliative_care, cognitive_status, incontinency, exercise_need, dysphagia_need,
                    living_circumstances, race_sensitivity, language_sensitivity, continuity_required, only_preferred,
                    title_id, nationality_id, religion_id, origin_id, area_id,
                    created_date, last_modified_date
                ) VALUES %s
                RETURNING id, name, lastname
            """
            
            template = ", ".join(["%s"] * 44)
            
            client_tuples = [
                (
                    client['name'],
                    client['lastname'],
                    client['middle_name'],
                    client['description'],
                    client['email'],
                    client['phone_number'],
                    client['mobile'],
                    client['website'],
                    client['termination_date'],
                    client['company_name'],
                    client['initial'],
                    client['preferred_name'],
                    client['hce_number'],
                    client['key_safe_number'],
                    client['access_details'],
                    client['consent_status'],
                    client['consent_date'],
                    client['consent_notes'],
                    client['status'],
                    client['gender'],
                    client['service_priority'],
                    client['start_date'],
                    client['birth_date'],
                    client['address_lines'],
                    client['town'],
                    client['postcode'],
                    client['disability'],
                    client['palliative_care'],
                    client['cognitive_status'],
                    client['incontinency'],
                    client['exercise_need'],
                    client['dysphagia_need'],
                    client['living_circumstances'],
                    client['race_sensitivity'],
                    client['language_sensitivity'],
                    client['continuity_required'],
                    client['only_preferred'],
                    client['title_id'],
                    client['nationality_id'],
                    client['religion_id'],
                    client['origin_id'],
                    client['area_id'],
                    client['created_date'],
                    client['updated_date'],
                )
                for client in to_insert
            ]
            
            execute_values(
                cursor,
                insert_query,
                client_tuples,
                template=f"({template})"
            )
            
            inserted = cursor.fetchall()
            processed.extend(inserted)
            logger.info(f"✓ Inserted {len(inserted)} new clients")
        
        # UPDATE existing clients
        for client, client_id in to_update:
            update_query = """
                UPDATE client SET
                    middle_name = %s,
                    description = %s,
                    email = %s,
                    phone_number = %s,
                    mobile = %s,
                    website = %s,
                    termination_date = %s,
                    company_name = %s,
                    initial = %s,
                    preferred_name = %s,
                    hce_number = %s,
                    key_safe_number = %s,
                    access_details = %s,
                    consent_status = %s,
                    consent_date = %s,
                    consent_notes = %s,
                    status = %s,
                    gender = %s,
                    service_priority = %s,
                    start_date = %s,
                    birth_date = %s,
                    address_lines = %s,
                    town = %s,
                    postcode = %s,
                    disability = %s,
                    palliative_care = %s,
                    cognitive_status = %s,
                    incontinency = %s,
                    exercise_need = %s,
                    dysphagia_need = %s,
                    living_circumstances = %s,
                    race_sensitivity = %s,
                    language_sensitivity = %s,
                    continuity_required = %s,
                    only_preferred = %s,
                    title_id = %s,
                    nationality_id = %s,
                    religion_id = %s,
                    origin_id = %s,
                    area_id = %s,
                    last_modified_date = NOW()
                WHERE id = %s
            """
            
            cursor.execute(update_query, (
                client['middle_name'],
                client['description'],
                client['email'],
                client['phone_number'],
                client['mobile'],
                client['website'],
                client['termination_date'],
                client['company_name'],
                client['initial'],
                client['preferred_name'],
                client['hce_number'],
                client['key_safe_number'],
                client['access_details'],
                client['consent_status'],
                client['consent_date'],
                client['consent_notes'],
                client['status'],
                client['gender'],
                client['service_priority'],
                client['start_date'],
                client['birth_date'],
                client['address_lines'],
                client['town'],
                client['postcode'],
                client['disability'],
                client['palliative_care'],
                client['cognitive_status'],
                client['incontinency'],
                client['exercise_need'],
                client['dysphagia_need'],
                client['living_circumstances'],
                client['race_sensitivity'],
                client['language_sensitivity'],
                client['continuity_required'],
                client['only_preferred'],
                client['title_id'],
                client['nationality_id'],
                client['religion_id'],
                client['origin_id'],
                client['area_id'],
                client_id,
            ))
            
            if cursor.rowcount:
                processed.append({
                    'id': client_id,
                    'name': client['name'],
                    'lastname': client['lastname'],
                })
        
        if to_update:
            logger.info(f"✓ Updated {len(to_update)} existing clients")
        
        # Deactivate clients that are in DB but not in the given file(s)
        if to_deactivate_ids:
            cursor.execute(
                "UPDATE client SET status = 'Deactive', last_modified_date = NOW() WHERE id = ANY(%s)",
                (to_deactivate_ids,),
            )
            logger.info(f"✓ Deactivated {len(to_deactivate_ids)} clients not present in file(s)")
        
        # Link clients to groups (many-to-many)
        if processed:
            logger.info(f"\nLinking {len(processed)} clients to groups...")
            link_clients_to_groups(cursor, processed, clients)
        
        connection.commit()
        
        logger.info(f"\n✓ Successfully processed {len(processed)} clients")
        
        # Show sample of processed clients
        logger.info("\nSample processed clients:")
        for i, row in enumerate(processed[:5]):
            logger.info(f"  - {row['name']} {row['lastname']} (ID: {row['id']})")
        if len(processed) > 5:
            logger.info(f"  ... and {len(processed) - 5} more")
        
        return True
        
    except Exception as e:
        connection.rollback()
        logger.error(f"\n✗ Failed to insert clients: {e}")
        raise
    finally:
        cursor.close()


def link_clients_to_groups(cursor, processed_clients, original_clients):
    """Link clients to their groups via many-to-many relationship"""
    
    # Create name+lastname to client mapping
    name_to_client = {
        f"{client['name']}|{client['lastname']}": client 
        for client in original_clients
    }
    
    # Prepare client-group links
    client_group_links = []
    for client_row in processed_clients:
        key = f"{client_row['name']}|{client_row['lastname']}"
        client_id = client_row['id']
        
        # Get group_id from original client data
        original_client = name_to_client.get(key)
        if original_client and original_client.get('group_id'):
            group_id = original_client['group_id']
            client_group_links.append((client_id, group_id))
    
    if not client_group_links:
        logger.warning("No client-group links to create")
        return
    
    # Delete existing links first to avoid duplicates
    client_ids = list(set(link[0] for link in client_group_links))
    cursor.execute(
        "DELETE FROM client_clients_groups WHERE client_id = ANY(%s)",
        (client_ids,)
    )
    
    # Insert into client_clients_groups junction table
    insert_groups_query = """
        INSERT INTO client_clients_groups (client_id, group_id)
        VALUES %s
    """
    
    execute_values(
        cursor,
        insert_groups_query,
        client_group_links,
        template="(%s, %s)"
    )
    
    logger.info(f"✓ Linked {len(client_group_links)} client-group relationships")


def run():
    """Main execution function"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Clients Migration                                ║
    ║         Seeding client table from CSV                    ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Get database config
    config = get_db_config()
    
    # Validate config
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in .env file")
        logger.error("Required: DB_NAME, DB_USER, DB_PASSWORD")
        return False
    
    # Get CSV path (uses exe dir when frozen, project root when dev)
    csv_path = get_assets_dir() / 'CustomerExport.csv'
    
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
        logger.info("STEP 2: SEED CLIENT GROUPS")
        logger.info("="*60)
        seed_client_groups_from_csv(connection, csv_path)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 3: SEED AREAS")
        logger.info("="*60)
        seed_areas_from_csv(connection, csv_path)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 4: LOAD LOOKUP TABLES")
        logger.info("="*60)
        lookups = get_lookup_tables(connection)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 5: EXTRACT CLIENTS FROM CSV")
        logger.info("="*60)
        clients = extract_clients_from_csv(csv_path, lookups)
        
        if not clients:
            logger.warning("No clients found in CSV")
            return False
        
        logger.info("\n" + "="*60)
        logger.info("STEP 6: SEED CLIENTS TO DATABASE")
        logger.info("="*60)
        success = seed_clients(connection, clients)
        
        if success:
            print("\n" + "="*60)
            print("✓ CLIENTS MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            return True
        else:
            print("\n" + "="*60)
            print("✗ CLIENTS MIGRATION FAILED")
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