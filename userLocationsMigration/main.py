"""
User Locations Migration
Updates user latitude and longitude from JSON backup
"""

import os
import json
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
        'port': int(os.getenv('DB_PORT', '6969')),
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


def load_user_locations_from_json(json_path):
    """Load user location data from JSON backup"""
    logger.info(f"Reading JSON: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    users = data.get('user', [])
    logger.info(f"Found {len(users)} users in JSON backup")
    
    # Extract location data
    user_locations = []
    skipped = 0
    
    for user in users:
        name = user.get('name', '').strip()
        lastname = user.get('lastname', '').strip()
        latitude = user.get('latitude')
        longitude = user.get('longitude')
        
        # Skip if missing required fields
        if not name or not lastname:
            skipped += 1
            continue
        
        # Skip if no location data
        if latitude is None or longitude is None:
            skipped += 1
            continue
        
        user_locations.append({
            'name': name,
            'lastname': lastname,
            'latitude': latitude,
            'longitude': longitude
        })
    
    logger.info(f"Extracted {len(user_locations)} users with location data")
    logger.info(f"Skipped {skipped} users (missing name/lastname or coordinates)")
    
    return user_locations


def update_user_locations(connection, user_locations):
    """Update user latitude and longitude in database"""
    if not user_locations:
        logger.warning("No user locations to update")
        return False
    
    logger.info(f"Updating locations for {len(user_locations)} users...")
    
    cursor = connection.cursor()
    try:
        updated_count = 0
        not_found_count = 0
        multiple_matches_count = 0
        failed_updates = []
        
        for user_data in user_locations:
            name = user_data['name']
            lastname = user_data['lastname']
            latitude = user_data['latitude']
            longitude = user_data['longitude']
            
            # Find user by name and lastname
            cursor.execute(
                """
                SELECT id, email 
                FROM "user" 
                WHERE name = %s AND lastname = %s
                """,
                (name, lastname)
            )
            
            matches = cursor.fetchall()
            
            if len(matches) == 0:
                not_found_count += 1
                logger.warning(f"User not found: {name} {lastname}")
                failed_updates.append({
                    'name': name,
                    'lastname': lastname,
                    'reason': 'not_found'
                })
                continue
            
            if len(matches) > 1:
                multiple_matches_count += 1
                logger.warning(f"Multiple users found for: {name} {lastname} ({len(matches)} matches)")
                failed_updates.append({
                    'name': name,
                    'lastname': lastname,
                    'reason': f'multiple_matches_{len(matches)}'
                })
                continue
            
            # Update the user
            user_id = matches[0]['id']
            cursor.execute(
                """
                UPDATE "user" 
                SET latitude = %s, 
                    longitude = %s,
                    last_modified_date = NOW()
                WHERE id = %s
                """,
                (latitude, longitude, user_id)
            )
            
            updated_count += 1
            
            if updated_count <= 5:
                logger.info(f"✓ Updated: {name} {lastname} (ID: {user_id}) -> ({latitude}, {longitude})")
        
        connection.commit()
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("UPDATE SUMMARY")
        logger.info("="*60)
        logger.info(f"✓ Successfully updated: {updated_count}")
        logger.info(f"✗ Not found in database: {not_found_count}")
        logger.info(f"⚠ Multiple matches (skipped): {multiple_matches_count}")
        logger.info(f"Total processed: {len(user_locations)}")
        
        if failed_updates:
            logger.info("\nFailed updates:")
            for failed in failed_updates[:10]:
                logger.info(f"  - {failed['name']} {failed['lastname']} ({failed['reason']})")
            if len(failed_updates) > 10:
                logger.info(f"  ... and {len(failed_updates) - 10} more")
        
        return updated_count > 0
        
    except Exception as e:
        connection.rollback()
        logger.error(f"\n✗ Failed to update user locations: {e}")
        raise
    finally:
        cursor.close()


def run():
    """Main execution function"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         User Locations Migration                         ║
    ║         Update lat/lng from JSON backup                  ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Get database config
    config = get_db_config()
    
    # Validate config
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration in .env file")
        logger.error("Required: DB_NAME, DB_USER, DB_PASSWORD")
        return False
    
    # Get JSON path
    json_path = Path(__file__).parent.parent / 'assets' / 'usersBackup.json'
    
    if not json_path.exists():
        logger.error(f"JSON file not found: {json_path}")
        logger.error("Please place usersBackup.json in the assets/ directory")
        return False
    
    # Connect and migrate
    connection = None
    try:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: DATABASE CONNECTION")
        logger.info("="*60)
        connection = connect_to_database(config)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 2: LOAD USER LOCATIONS FROM JSON")
        logger.info("="*60)
        user_locations = load_user_locations_from_json(json_path)
        
        if not user_locations:
            logger.warning("No user locations found in JSON")
            return False
        
        logger.info("\n" + "="*60)
        logger.info("STEP 3: UPDATE USER LOCATIONS IN DATABASE")
        logger.info("="*60)
        success = update_user_locations(connection, user_locations)
        
        if success:
            print("\n" + "="*60)
            print("✓ USER LOCATIONS MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            return True
        else:
            print("\n" + "="*60)
            print("✗ USER LOCATIONS MIGRATION FAILED")
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