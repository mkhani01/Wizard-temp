"""
Shared database connection manager for the migration wizard.
- Wraps psycopg2 with health checking and reconnection.
- Uses TCP keepalives to detect dead connections (e.g. dropped port-forward).
- Cross-platform: no signals, only threading primitives.
"""

import logging
import threading

import psycopg2
from psycopg2 import OperationalError, InterfaceError
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# Keepalive settings (work on Windows, macOS, Linux)
KEEPALIVE_KWARGS = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}


class ConnectionLostError(Exception):
    """
    Raised when the database connection is lost during a migration step.
    The wizard catches this and shows "Database connection lost" + Retry UI.
    """
    def __init__(self, step_name, context=None):
        self.step_name = step_name
        self.context = context or {}
        super().__init__(f"Database connection lost during: {step_name}")


class ConnectionManager:
    """
    Manages a single PostgreSQL connection for the migration run.
    - get_connection(): returns the current connection, creating one if needed (with keepalives).
    - check_connection(): runs SELECT 1 to verify the connection is alive.
    - close(): closes the current connection; next get_connection() will create a new one.
    Thread-safe for a single migration run (one thread uses it at a time in _do_run).
    """

    def __init__(self, config):
        """
        config: dict with host, port, database, user, password (port may be int or str).
        """
        self._config = dict(config)
        port = self._config.get("port", 5432)
        self._config["port"] = int(port) if isinstance(port, str) else port
        self._connection = None
        self._lock = threading.Lock()

    def get_connection(self):
        """Return the current connection; create one if none or closed."""
        with self._lock:
            if self._connection is not None and self._connection.closed == 0:
                return self._connection
            self._connection = self._connect()
            return self._connection

    def _connect(self):
        """Create a new connection with keepalives."""
        logger.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=self._config["host"],
            port=self._config["port"],
            database=self._config["database"],
            user=self._config["user"],
            password=self._config["password"],
            cursor_factory=RealDictCursor,
            connect_timeout=10,
            **KEEPALIVE_KWARGS,
        )
        conn.autocommit = False
        logger.info("Connected to database")
        return conn

    def check_connection(self):
        """
        Run SELECT 1 to verify the connection is alive.
        Returns True if ok, False otherwise. Does not raise.
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            return True
        except (OperationalError, InterfaceError) as e:
            logger.warning("Connection check failed: %s", e)
            return False

    def close(self):
        """Close the current connection. Next get_connection() will create a new one."""
        with self._lock:
            if self._connection is not None and self._connection.closed == 0:
                try:
                    self._connection.close()
                except Exception as e:
                    logger.warning("Error closing connection: %s", e)
                self._connection = None


def is_connection_error(exc):
    """Return True if the exception indicates a lost/bad database connection."""
    if isinstance(exc, (OperationalError, InterfaceError)):
        return True
    # AdminShutdown is under psycopg2.errors
    try:
        import psycopg2.errors
        if type(exc).__name__ == "AdminShutdown" and type(exc).__module__.startswith("psycopg2"):
            return True
    except Exception:
        pass
    return False
