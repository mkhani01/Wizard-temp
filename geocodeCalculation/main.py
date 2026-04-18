"""
Geocode Calculation Migration
Geocodes postcodes and assigns H3 hexagons for users and clients
"""

import os
import sys
import json
import signal
import logging
import hashlib
import time
import math
import threading
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import h3

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

# Configuration
POPULATION_THRESHOLD = 999
CITY_RADIUS_KM = 5
DENSE_H3_RESOLUTION = 9
RURAL_H3_RESOLUTION = 7

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    global shutdown_requested
    logger.warning("\n⚠️  Shutdown requested (Ctrl+C). Finishing current operation and saving cache...")
    shutdown_requested = True


# Register signal handler only in main thread (signal.signal() is main-thread-only).
# When run from the wizard, migrations run in a worker thread, so skip registration.
if threading.current_thread() is threading.main_thread():
    try:
        signal.signal(signal.SIGINT, signal_handler)
    except ValueError:
        pass  # e.g. not available in some environments


def get_db_config():
    """Get database configuration from environment variables"""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '5432')),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }


try:
    from connection_manager import ConnectionLostError
except ImportError:
    ConnectionLostError = None

KEEPALIVES = dict(keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)


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


class GeocodeCache:
    """Manages geocoding cache in .cache/geocode (under project/exe root so it works when frozen)."""
    
    def __init__(self, cache_dir=None):
        if cache_dir is None:
            from migration_support import get_project_root
            cache_dir = get_project_root() / ".cache" / "geocode"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir.absolute()}")
    
    def _get_cache_key(self, postcode):
        """Generate cache key from postcode"""
        normalized = postcode.strip().upper().replace(' ', '')
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def get(self, postcode):
        """Get cached geocoding result"""
        cache_key = self._get_cache_key(postcode)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to read cache for {postcode}: {e}")
        
        return None
    
    def set(self, postcode, geocode_data):
        """Cache geocoding result"""
        cache_key = self._get_cache_key(postcode)
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        try:
            data = {
                'postcode': postcode,
                'full_response': geocode_data,
                'timestamp': time.time()
            }
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to cache {postcode}: {e}")


class GoogleGeocoder:
    """Geocodes postcodes using Google Maps Geocoding API"""
    
    def __init__(self, api_key, cache):
        self.api_key = api_key
        self.cache = cache
        self.request_count = 0
        self.cache_hits = 0
    
    def geocode(self, postcode, country='IE'):
        """Geocode a postcode (Ireland by default)"""
        if not postcode or postcode.strip() == '':
            return None

        # Check cache first
        cached = self.cache.get(postcode)
        if cached:
            self.cache_hits += 1
            full_response = cached.get('full_response', {})
            # Only use cache if it was successful
            if full_response.get('status') == 'OK' and full_response.get('results'):
                return full_response
            # If cached response was an error, try again (don't return None)
            elif full_response.get('status') in ['REQUEST_DENIED', 'INVALID_REQUEST', 'UNKNOWN_ERROR']:
                logger.info(f"Cached error for {postcode}, retrying...")
                # Continue to API call below instead of returning None
            else:
                return None

        # Check for shutdown request
        if shutdown_requested:
            logger.warning(f"Shutdown requested, skipping geocoding for: {postcode}")
            return None

        # Make API request
        try:
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                'address': postcode,
                'components': f'country:{country}',
                'key': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            self.request_count += 1
            
            # Only cache successful responses
            if data['status'] == 'OK' and len(data['results']) > 0:
                self.cache.set(postcode, data)
                # Rate limiting
                time.sleep(0.1)
                return data
            else:
                # Don't cache errors - log and return None
                logger.warning(f"Geocoding failed for {postcode}: {data.get('status')} - {data.get('error_message', 'No error message')}")
                return None
                
        except Exception as e:
            logger.error(f"Error geocoding {postcode}: {e}")
            return None

def load_irish_cities(ie_file_path):
    """Load Irish cities with population >= threshold from IE.txt"""
    cities = []
    
    logger.info(f"Loading Irish cities from: {ie_file_path}")
    
    with open(ie_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) < 19:
                continue
            
            # Check if it's a populated place (PPL)
            feature_class = parts[6] if len(parts) > 6 else ''
            feature_code = parts[7] if len(parts) > 7 else ''
            
            if feature_class == 'P' and feature_code == 'PPL':
                try:
                    name = parts[1]
                    lat = float(parts[4])
                    lng = float(parts[5])
                    population = int(parts[14]) if parts[14] else 0
                    
                    if population >= POPULATION_THRESHOLD:
                        cities.append({
                            'name': name,
                            'lat': lat,
                            'lng': lng,
                            'population': population
                        })
                except (ValueError, IndexError):
                    continue
    
    logger.info(f"Loaded {len(cities)} cities with population >= {POPULATION_THRESHOLD}")
    return cities


def haversine_distance(lat1, lng1, lat2, lng2):
    """Calculate distance between two points in kilometers using Haversine formula"""
    R = 6371  # Earth's radius in kilometers
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


def is_in_dense_area(lat, lng, cities):
    """Check if location is within CITY_RADIUS_KM of any city"""
    for city in cities:
        distance = haversine_distance(lat, lng, city['lat'], city['lng'])
        if distance <= CITY_RADIUS_KM:
            return True, city['name']
    return False, None


def get_h3_hexagon_for_dense_check(lat, lng, cities):
    """
    Determine H3 hexagon with overlap detection
    
    Option A: If a Resolution 7 hexagon overlaps with ANY dense zone,
    use Resolution 9 for the entire area
    """
    # First check if the point itself is in dense area
    in_dense, city_name = is_in_dense_area(lat, lng, cities)
    
    if in_dense:
        # Point is in dense area, use high resolution
        h3_hex = h3.latlng_to_cell(lat, lng, DENSE_H3_RESOLUTION)
        return h3_hex, DENSE_H3_RESOLUTION, city_name
    
    # Point is in rural area, check if Resolution 7 hex overlaps with any dense zone
    rural_hex = h3.latlng_to_cell(lat, lng, RURAL_H3_RESOLUTION)
    
    # Get the boundary of this Resolution 7 hexagon
    hex_boundary = h3.cell_to_boundary(rural_hex)
    
    # Check if any point on the hexagon boundary is in a dense area
    for boundary_lat, boundary_lng in hex_boundary:
        in_dense_boundary, _ = is_in_dense_area(boundary_lat, boundary_lng, cities)
        if in_dense_boundary:
            # Overlap detected! Use Resolution 9 instead
            h3_hex = h3.latlng_to_cell(lat, lng, DENSE_H3_RESOLUTION)
            return h3_hex, DENSE_H3_RESOLUTION, "overlap_with_dense"
    
    # Also check the center of the hexagon
    hex_center = h3.cell_to_latlng(rural_hex)
    in_dense_center, _ = is_in_dense_area(hex_center[0], hex_center[1], cities)
    if in_dense_center:
        h3_hex = h3.latlng_to_cell(lat, lng, DENSE_H3_RESOLUTION)
        return h3_hex, DENSE_H3_RESOLUTION, "overlap_with_dense"
    
    # No overlap, safe to use Resolution 7
    return rural_hex, RURAL_H3_RESOLUTION, None


def _env_flag(name):
    """Parse boolean env flag (1/true/yes/on)."""
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def get_users_with_postcodes(connection, include_with_coordinates=False):
    """Get users with postcodes. Default only missing coordinates; optional full refresh."""
    cursor = connection.cursor()
    try:
        if include_with_coordinates:
            query = """
                SELECT id, name, lastname, postcode
                FROM "user"
                WHERE postcode IS NOT NULL AND postcode != ''
                ORDER BY id
            """
            cursor.execute(query)
            users = cursor.fetchall()
            logger.info(f"Found {len(users)} users with postcodes (full re-geocode mode)")
            return users
        query = """
            SELECT id, name, lastname, postcode
            FROM "user"
            WHERE postcode IS NOT NULL AND postcode != ''
            AND (latitude IS NULL OR longitude IS NULL)
            ORDER BY id
        """
        cursor.execute(query)
        users = cursor.fetchall()
        logger.info(f"Found {len(users)} users with postcodes but no coordinates")
        return users
    finally:
        cursor.close()


def get_clients_with_postcodes(connection, include_with_coordinates=False):
    """Get clients with postcodes. Default only missing coordinates; optional full refresh."""
    cursor = connection.cursor()
    try:
        if include_with_coordinates:
            query = """
                SELECT id, name, lastname, postcode
                FROM client
                WHERE postcode IS NOT NULL AND postcode != ''
                ORDER BY id
            """
            cursor.execute(query)
            clients = cursor.fetchall()
            logger.info(f"Found {len(clients)} clients with postcodes (full re-geocode mode)")
            return clients
        query = """
            SELECT id, name, lastname, postcode
            FROM client
            WHERE postcode IS NOT NULL AND postcode != ''
            AND (latitude IS NULL OR longitude IS NULL)
            ORDER BY id
        """
        cursor.execute(query)
        clients = cursor.fetchall()
        logger.info(f"Found {len(clients)} clients with postcodes but no coordinates")
        return clients
    finally:
        cursor.close()


def get_users_without_postcodes(connection):
    """Get users without postcodes AND without coordinates (cannot be geocoded)"""
    cursor = connection.cursor()
    try:
        query = """
            SELECT id, name, lastname
            FROM "user"
            WHERE (postcode IS NULL OR postcode = '')
            AND (latitude IS NULL OR longitude IS NULL)
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()


def get_clients_without_postcodes(connection):
    """Get clients without postcodes AND without coordinates (cannot be geocoded)"""
    cursor = connection.cursor()
    try:
        query = """
            SELECT id, name, lastname
            FROM client
            WHERE (postcode IS NULL OR postcode = '')
            AND (latitude IS NULL OR longitude IS NULL)
        """
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()


def update_user_geocode(connection, user_id, latitude, longitude, h3_hexagon):
    """Update user location and H3 hexagon"""
    cursor = connection.cursor()
    try:
        query = """
            UPDATE "user"
            SET latitude = %s, longitude = %s, h3_hexagon = %s, last_modified_date = NOW()
            WHERE id = %s
        """
        cursor.execute(query, (latitude, longitude, h3_hexagon, user_id))
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error(f"Failed to update user {user_id}: {e}")
        raise
    finally:
        cursor.close()


def update_client_geocode(connection, client_id, latitude, longitude, h3_hexagon):
    """Update client location and H3 hexagon"""
    cursor = connection.cursor()
    try:
        query = """
            UPDATE client
            SET latitude = %s, longitude = %s, h3_hexagon = %s, last_modified_date = NOW()
            WHERE id = %s
        """
        cursor.execute(query, (latitude, longitude, h3_hexagon, client_id))
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error(f"Failed to update client {client_id}: {e}")
        raise
    finally:
        cursor.close()


def process_users(connection, geocoder, users, cities):
    """Process and geocode users"""
    if not users:
        logger.info("No users to process")
        return 0
    
    logger.info(f"\nProcessing {len(users)} users...")
    
    success_count = 0
    failed_count = 0
    stats = {
        'dense': 0,
        'rural': 0,
        'overlap': 0
    }
    
    for i, user in enumerate(users, 1):
        if shutdown_requested:
            logger.warning(f"\n⚠️  Shutdown requested. Processed {i-1}/{len(users)} users.")
            break
        
        user_id = user['id']
        name = user['name']
        lastname = user['lastname']
        postcode = user['postcode']
        
        logger.info(f"[{i}/{len(users)}] User: {name} {lastname} (ID: {user_id}) - {postcode}")
        
        # Geocode
        geocode_result = geocoder.geocode(postcode)
        
        if not geocode_result or not geocode_result.get('results'):
            failed_count += 1
            logger.warning(f"  ✗ Failed to geocode")
            continue
        
        # Extract coordinates
        location = geocode_result['results'][0]['geometry']['location']
        lat = location['lat']
        lng = location['lng']
        
        # Determine H3 hexagon with overlap detection
        h3_hex, resolution, reason = get_h3_hexagon_for_dense_check(lat, lng, cities)
        
        # Update database
        update_user_geocode(connection, user_id, lat, lng, h3_hex)
        
        success_count += 1
        
        if resolution == DENSE_H3_RESOLUTION:
            if reason == 'overlap_with_dense':
                stats['overlap'] += 1
                logger.info(f"  ✓ {lat:.6f}, {lng:.6f} → Res {resolution} (overlap)")
            else:
                stats['dense'] += 1
                logger.info(f"  ✓ {lat:.6f}, {lng:.6f} → Res {resolution} (near {reason})")
        else:
            stats['rural'] += 1
            logger.info(f"  ✓ {lat:.6f}, {lng:.6f} → Res {resolution} (rural)")
    
    logger.info(f"\nUsers processing summary:")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"  Dense (Res {DENSE_H3_RESOLUTION}): {stats['dense']}")
    logger.info(f"  Rural (Res {RURAL_H3_RESOLUTION}): {stats['rural']}")
    logger.info(f"  Overlap → Dense (Res {DENSE_H3_RESOLUTION}): {stats['overlap']}")
    
    return success_count


def process_clients(connection, geocoder, clients, cities):
    """Process and geocode clients"""
    if not clients:
        logger.info("No clients to process")
        return 0
    
    logger.info(f"\nProcessing {len(clients)} clients...")
    
    success_count = 0
    failed_count = 0
    stats = {
        'dense': 0,
        'rural': 0,
        'overlap': 0
    }
    
    for i, client in enumerate(clients, 1):
        if shutdown_requested:
            logger.warning(f"\n⚠️  Shutdown requested. Processed {i-1}/{len(clients)} clients.")
            break
        
        client_id = client['id']
        name = client['name']
        lastname = client['lastname']
        postcode = client['postcode']
        
        logger.info(f"[{i}/{len(clients)}] Client: {name} {lastname} (ID: {client_id}) - {postcode}")
        
        # Geocode
        geocode_result = geocoder.geocode(postcode)
        
        if not geocode_result or not geocode_result.get('results'):
            failed_count += 1
            logger.warning(f"  ✗ Failed to geocode")
            continue
        
        # Extract coordinates
        location = geocode_result['results'][0]['geometry']['location']
        lat = location['lat']
        lng = location['lng']
        
        # Determine H3 hexagon with overlap detection
        h3_hex, resolution, reason = get_h3_hexagon_for_dense_check(lat, lng, cities)
        
        # Update database
        update_client_geocode(connection, client_id, lat, lng, h3_hex)
        
        success_count += 1
        
        if resolution == DENSE_H3_RESOLUTION:
            if reason == 'overlap_with_dense':
                stats['overlap'] += 1
                logger.info(f"  ✓ {lat:.6f}, {lng:.6f} → Res {resolution} (overlap)")
            else:
                stats['dense'] += 1
                logger.info(f"  ✓ {lat:.6f}, {lng:.6f} → Res {resolution} (near {reason})")
        else:
            stats['rural'] += 1
            logger.info(f"  ✓ {lat:.6f}, {lng:.6f} → Res {resolution} (rural)")
    
    logger.info(f"\nClients processing summary:")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"  Dense (Res {DENSE_H3_RESOLUTION}): {stats['dense']}")
    logger.info(f"  Rural (Res {RURAL_H3_RESOLUTION}): {stats['rural']}")
    logger.info(f"  Overlap → Dense (Res {DENSE_H3_RESOLUTION}): {stats['overlap']}")
    
    return success_count


def run(connection_manager=None, state=None):
    """Main execution function. connection_manager and state used from wizard for resume support."""
    import psycopg2
    geocode_all_users = _env_flag("GEOCODE_ALL_USERS")
    geocode_all_clients = _env_flag("GEOCODE_ALL_CLIENTS")
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Geocode Calculation Migration                    ║
    ║         Postcode → Lat/Lng → H3 Hexagon                  ║
    ║         (Missing coords or full re-geocode mode)         ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    if state and state.is_completed("geocode_api"):
        logger.info("Geocode migration already completed (resume).")
        return True
    logger.info("Configuration:")
    logger.info("  Population Threshold: %s", POPULATION_THRESHOLD)
    logger.info("  City Radius: %s km", CITY_RADIUS_KM)
    logger.info("  Dense Resolution: %s", DENSE_H3_RESOLUTION)
    logger.info("  Rural Resolution: %s", RURAL_H3_RESOLUTION)
    google_api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not google_api_key:
        logger.error("GOOGLE_MAPS_API_KEY not found in environment variables")
        return False
    config = get_db_config()
    if not all([config['database'], config['user'], config['password']]):
        logger.error("Missing database configuration")
        return False
    from migration_support import get_assets_dir
    ie_file = get_assets_dir() / 'IE.txt'
    if not ie_file.exists():
        logger.error("IE.txt not found: %s", ie_file)
        return False
    cache = GeocodeCache()
    geocoder = GoogleGeocoder(google_api_key, cache)
    connection = None
    try:
        logger.info("\n" + "="*60)
        logger.info("STEP 1: LOAD IRISH CITIES")
        logger.info("="*60)
        cities = load_irish_cities(ie_file)
        logger.info("\n" + "="*60)
        logger.info("STEP 2: DATABASE CONNECTION")
        logger.info("="*60)
        if connection_manager:
            connection = connection_manager.get_connection()
        else:
            connection = connect_to_database(config)
        
        logger.info("\n" + "="*60)
        logger.info("STEP 3: FETCH RECORDS")
        logger.info("="*60)
        logger.info("Fetching users and clients that need geocoding...")
        if geocode_all_users or geocode_all_clients:
            logger.info("Full re-geocode mode enabled:")
            logger.info("  - Users:   %s", "ALL with postcode" if geocode_all_users else "ONLY missing coordinates")
            logger.info("  - Clients: %s", "ALL with postcode" if geocode_all_clients else "ONLY missing coordinates")
        else:
            logger.info("Default mode:")
            logger.info("  - WITH postcode but WITHOUT coordinates → will be geocoded")
        logger.info("  - WITHOUT postcode and WITHOUT coordinates → will be logged as warnings")
        logger.info("")
        users = get_users_with_postcodes(connection, include_with_coordinates=geocode_all_users)
        clients = get_clients_with_postcodes(connection, include_with_coordinates=geocode_all_clients)
        users_no_postcode = get_users_without_postcodes(connection)
        clients_no_postcode = get_clients_without_postcodes(connection)

        logger.info(f"Summary:")
        logger.info(f"  - {len(users)} users to geocode")
        logger.info(f"  - {len(clients)} clients to geocode")
        logger.info(f"  - {len(users_no_postcode)} users without postcodes (will skip)")
        logger.info(f"  - {len(clients_no_postcode)} clients without postcodes (will skip)")
        
        logger.info("\n" + "="*60)
        logger.info("STEP 4: PROCESS USERS")
        logger.info("="*60)
        users_success = process_users(connection, geocoder, users, cities)
        
        if shutdown_requested:
            logger.warning("\n⚠️  Shutdown requested. Stopping before clients.")
        else:
            logger.info("\n" + "="*60)
            logger.info("STEP 5: PROCESS CLIENTS")
            logger.info("="*60)
            clients_success = process_clients(connection, geocoder, clients, cities)
        
        # Log records without postcodes (and missing coordinates - cannot be geocoded)
        if users_no_postcode or clients_no_postcode:
            logger.warning("\n" + "="*60)
            logger.warning("⚠️  RECORDS MISSING POSTCODES (CANNOT BE GEOCODED)")
            logger.warning("="*60)
            logger.warning("These records have no postcode and no coordinates.")
            logger.warning("They cannot be geocoded and need manual address entry.")

            if users_no_postcode:
                logger.warning(f"\n{len(users_no_postcode)} Users without postcodes:")
                for user in users_no_postcode[:20]:
                    logger.warning(f"  - User ID {user['id']}: {user['name']} {user['lastname']}")
                if len(users_no_postcode) > 20:
                    logger.warning(f"  ... and {len(users_no_postcode) - 20} more")

            if clients_no_postcode:
                logger.warning(f"\n{len(clients_no_postcode)} Clients without postcodes:")
                for client in clients_no_postcode[:20]:
                    logger.warning(f"  - Client ID {client['id']}: {client['name']} {client['lastname']}")
                if len(clients_no_postcode) > 20:
                    logger.warning(f"  ... and {len(clients_no_postcode) - 20} more")
        
        # Final statistics
        logger.info("\n" + "="*60)
        logger.info("FINAL STATISTICS")
        logger.info("="*60)
        logger.info(f"API Requests Made: {geocoder.request_count}")
        logger.info(f"Cache Hits: {geocoder.cache_hits}")
        logger.info(f"Cache Directory: {cache.cache_dir.absolute()}")
        logger.info(f"Cities Loaded: {len(cities)}")
        
        if shutdown_requested:
            print("\n" + "="*60)
            print("⚠️  MIGRATION INTERRUPTED (Ctrl+C)")
            print("✓ Cache saved. You can resume by running again.")
            print("="*60)
            return False
        else:
            if state:
                state.clear_step("geocode_api")
            print("\n" + "="*60)
            print("✓ GEOCODE CALCULATION COMPLETED SUCCESSFULLY")
            print("="*60)
            return True
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        if ConnectionLostError:
            raise ConnectionLostError("geocode_api", {}) from e
        raise
    except Exception as e:
        logger.error("Migration error: %s", e, exc_info=True)
        return False
    finally:
        if connection and not connection_manager:
            connection.close()
            logger.info("\nDatabase connection closed")


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
