"""
Client Locations Migration
Updates client latitude and longitude from JSON backup
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import OperationalError, InterfaceError

try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

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


def load_client_locations_from_json(json_path):
    """Load client location data from JSON backup"""
    logger.info(f"Reading JSON: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    clients = data.get('client', [])
    logger.info(f"Found {len(clients)} clients in JSON backup")
    
    # Extract location data
    client_locations = []
    skipped = 0

    for idx, client in enumerate(clients):
        name = client.get('name', '').strip()
        lastname = client.get('lastname', '').strip()
        latitude = client.get('latitude')
        longitude = client.get('longitude')

        if not name or not lastname:
            skipped += 1
            logger.warning(
                "Item %d: SKIPPED - missing name/lastname | name=%r, lastname=%r, lat=%r, long=%r",
                idx + 1, name, lastname, latitude, longitude
            )
            continue

        if latitude is None or longitude is None:
            skipped += 1
            logger.warning(
                "Item %d: SKIPPED - missing coordinates | name=%r, lastname=%r, lat=%r, long=%r",
                idx + 1, name, lastname, latitude, longitude
            )
            continue

        client_locations.append({
            'name': name,
            'lastname': lastname,
            'latitude': latitude,
            'longitude': longitude
        })
        logger.info("Item %d: ADDED | name=%r, lastname=%r, lat=%s, long=%s", idx + 1, name, lastname, latitude, longitude)

    logger.info("Extracted %d clients with location data; skipped %d", len(client_locations), skipped)
    
    return client_locations


def update_client_locations(connection, client_locations):
    """Update client latitude and longitude in database"""
    if not client_locations:
        logger.warning("No client locations to update")
        return False
    
    logger.info(f"Updating locations for {len(client_locations)} clients...")
    
    cursor = connection.cursor()
    try:
        updated_count = 0
        not_found_count = 0
        multiple_matches_count = 0
        failed_updates = []
        
        for client_data in client_locations:
            name = client_data['name']
            lastname = client_data['lastname']
            latitude = client_data['latitude']
            longitude = client_data['longitude']
            
            # Find client by name and lastname
            cursor.execute(
                """
                SELECT id 
                FROM client 
                WHERE name = %s AND lastname = %s
                """,
                (name, lastname)
            )
            
            matches = cursor.fetchall()
            
            if len(matches) == 0:
                not_found_count += 1
                logger.warning("SEED SKIP - client not found | name=%r, lastname=%r, lat=%s, long=%s", name, lastname, latitude, longitude)
                failed_updates.append({
                    'name': name,
                    'lastname': lastname,
                    'reason': 'not_found'
                })
                continue
            
            if len(matches) > 1:
                multiple_matches_count += 1
                logger.warning("SEED SKIP - multiple clients found | name=%r, lastname=%r, matches=%d", name, lastname, len(matches))
                failed_updates.append({
                    'name': name,
                    'lastname': lastname,
                    'reason': 'multiple_matches_%d' % len(matches)
                })
                continue
            
            # Update the client
            client_id = matches[0]['id']
            cursor.execute(
                """
                UPDATE client 
                SET latitude = %s, 
                    longitude = %s,
                    last_modified_date = NOW()
                WHERE id = %s
                """,
                (latitude, longitude, client_id)
            )
            
            updated_count += 1
            logger.info("  SEEDED client location id=%s name=%r lastname=%r lat=%s long=%s", client_id, name, lastname, latitude, longitude)

        connection.commit()
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("UPDATE SUMMARY")
        logger.info("="*60)
        logger.info(f"✓ Successfully updated: {updated_count}")
        logger.info(f"✗ Not found in database: {not_found_count}")
        logger.info(f"⚠ Multiple matches (skipped): {multiple_matches_count}")
        logger.info(f"Total processed: {len(client_locations)}")
        
        if failed_updates:
            logger.info("\nFailed updates:")
            for failed in failed_updates[:10]:
                logger.info(f"  - {failed['name']} {failed['lastname']} ({failed['reason']})")
            if len(failed_updates) > 10:
                logger.info(f"  ... and {len(failed_updates) - 10} more")
        
        return updated_count > 0
        
    except Exception as e:
        connection.rollback()
        logger.error(f"\n✗ Failed to update client locations: {e}")
        raise
    finally:
        cursor.close()


def run(connection_manager=None, state=None):
    """Main execution function. connection_manager and state used from wizard for resume support."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Client Locations Migration                       ║
    ║         Update lat/lng from JSON backup                  ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    if state and state.is_completed("client_locations"):
        logger.info("Client locations migration already completed (resume).")
        return True
    config = get_db_config()
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in .env file")
        logger.error("Required: DB_NAME, DB_USER, DB_PASSWORD")
        return False
    from migration_support import get_assets_dir
    json_path = get_assets_dir() / 'clientbackup.json'
    if not json_path.exists():
        logger.error("JSON file not found: %s", json_path)
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
        logger.info("STEP 2: LOAD CLIENT LOCATIONS FROM JSON")
        logger.info("="*60)
        client_locations = load_client_locations_from_json(json_path)
        if not client_locations:
            logger.warning("No client locations found in JSON")
            return False
        logger.info("\n" + "="*60)
        logger.info("STEP 3: UPDATE CLIENT LOCATIONS IN DATABASE")
        logger.info("="*60)
        success = update_client_locations(connection, client_locations)
        if success:
            if state:
                state.clear_step("client_locations")
            print("\n" + "="*60)
            print("✓ CLIENT LOCATIONS MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            return True
        print("\n" + "="*60)
        print("✗ CLIENT LOCATIONS MIGRATION FAILED")
        print("="*60)
        return False
    except (OperationalError, InterfaceError) as e:
        if ConnectionLostError:
            raise ConnectionLostError("client_locations", {}) from e
        raise
    except Exception as e:
        logger.error("Migration error: %s", e, exc_info=True)
        return False
    finally:
        if connection and not connection_manager:
            connection.close()
            logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    import sys
    success = run()
    sys.exit(0 if success else 1)