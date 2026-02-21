"""
Areas Migration
Seeds users_group table with areas from CSV
"""

import os
import csv
import logging
from pathlib import Path
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
        'port': int(os.getenv('DB_PORT', '7070')),
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


def extract_areas_from_csv(csv_path):
    """Extract unique areas from CSV"""
    areas = set()
    
    logger.info("Reading CSV: %s", csv_path)
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        row_num = 0
        for row in reader:
            row_num += 1
            area = row.get('Area', '').strip()
            if area:
                areas.add(area)
                logger.info("Row %d: ADDED area | Area=%r", row_num, area)
            else:
                logger.warning(
                    "Row %d: SKIPPED - empty Area | row keys=%s",
                    row_num, list(row.keys())
                )
    
    logger.info("Found %d unique areas:", len(areas))
    for area in sorted(areas):
        logger.info("  - %s", area)
    
    return areas


def get_existing_areas(connection):
    """Get existing areas from database"""
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT id, name FROM users_group")
        existing = {row['name']: row['id'] for row in cursor.fetchall()}
        
        if existing:
            logger.info(f"Found {len(existing)} existing areas in database:")
            for name, area_id in sorted(existing.items()):
                logger.info(f"  - {name} (ID: {area_id})")
        else:
            logger.info("No existing areas found in database")
        
        return existing
    finally:
        cursor.close()


def seed_areas(connection, areas):
    """Insert areas into users_group table"""
    # Get existing areas
    existing_areas = get_existing_areas(connection)
    
    # Find new areas
    new_areas = areas - set(existing_areas.keys())
    
    if not new_areas:
        logger.info("\n✓ All areas already exist in database. No new areas to add.")
        return True
    
    logger.info(f"\nInserting {len(new_areas)} new areas...")
    
    # Prepare data
    area_data = [(area, f"Team {area}") for area in sorted(new_areas)]
    
    # Insert
    cursor = connection.cursor()
    try:
        insert_query = """
            INSERT INTO users_group (name, description, created_date, last_modified_date)
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
        
        logger.info("Successfully inserted %d areas:", len(inserted))
        for row in inserted:
            logger.info("  SEEDED area id=%s name=%r", row['id'], row['name'])
        
        return True
        
    except Exception as e:
        connection.rollback()
        logger.error(f"\n✗ Failed to insert areas: {e}")
        raise
    finally:
        cursor.close()


def run():
    """Main execution function"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Area Migration                                   ║
    ║         Seeding users_group table from CSV               ║
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
    csv_path = get_assets_dir() / 'CareAssistantExport.csv'
    
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
        logger.info("STEP 2: EXTRACT AREAS FROM CSV")
        logger.info("="*60)
        areas = extract_areas_from_csv(csv_path)
        
        if not areas:
            logger.warning("No areas found in CSV")
            return False
        
        logger.info("\n" + "="*60)
        logger.info("STEP 3: SEED AREAS TO DATABASE")
        logger.info("="*60)
        success = seed_areas(connection, areas)
        
        if success:
            print("\n" + "="*60)
            print("✓ AREA MIGRATION COMPLETED SUCCESSFULLY")
            print("="*60)
            return True
        else:
            print("\n" + "="*60)
            print("✗ AREA MIGRATION FAILED")
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