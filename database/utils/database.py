from psycopg import Cursor


def truncate_tables(cursor: Cursor, tables: list[str]) -> None:
    """
    Truncates tables and resets identity values.
    """

    for table in tables:
        cursor.execute(
            f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"
        )

"""
PostgreSQL connection wrapper.
Uses existing database.config.db_config for connections.
"""

import logging
from contextlib import contextmanager
from database.config.db_config import get_connection

logger = logging.getLogger(__name__)


class PostgresConnection:
    """Wrapper around PostgreSQL connection using existing config"""
    
    def __init__(self):
        """
        Initialize using existing database config.
        No need to pass credentials - uses environment variables via settings.py
        """
        logger.info("PostgreSQL connection initialized (using existing config)")
    
    def query(self, sql, params=None):
        """
        Execute SELECT query and return all results
        
        Args:
            sql: SQL query string
            params: Query parameters (tuple or list)
        
        Returns:
            List of tuples (one per row)
        """
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params or ())
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            conn.close()
    
    def query_one(self, sql, params=None):
        """
        Execute SELECT query and return first result
        
        Args:
            sql: SQL query string
            params: Query parameters
        
        Returns:
            Single tuple or None
        """
        results = self.query(sql, params)
        return results[0] if results else None
    
    def execute(self, sql, params=None):
        """
        Execute INSERT/UPDATE/DELETE query
        
        Args:
            sql: SQL query string
            params: Query parameters
        
        Returns:
            Number of rows affected
        """
        conn = get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params or ())
            rows_affected = cursor.rowcount
            conn.commit()
            return rows_affected
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cursor.close()
            conn.close()