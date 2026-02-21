"""
Database configuration and connection for Availability Types migration.
Uses environment variables: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.
"""

import os
import logging

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def get_db_config():
    """Get database configuration from environment variables."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }


def connect_to_database(config):
    """Connect to PostgreSQL database."""
    try:
        logger.info("Connecting to PostgreSQL...")
        logger.info("  Host: %s", config["host"])
        logger.info("  Port: %s", config["port"])
        logger.info("  Database: %s", config["database"])
        logger.info("  User: %s", config["user"])

        connection = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
        connection.autocommit = False
        logger.info("✓ Connected to database")
        return connection
    except Exception as e:
        logger.error("✗ Failed to connect: %s", e)
        raise
