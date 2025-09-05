# database.py - FOLLOWS coding standards
import sqlite3
import logging
import os
from typing import Optional, List, Tuple
from contextlib import contextmanager


class DatabaseConnection:
    """
    Manages SQLite database connections and basic operations.
    
    This class provides a secure way to connect to and interact with 
    the SQLite database, ensuring proper resource management and 
    credential handling through environment variables.
    """
    
    def __init__(self):
        self.dbPath = os.getenv('DATABASE_PATH', 'users.db')
        self.connectionTimeout = int(os.getenv('DB_TIMEOUT', '30'))
        
    @contextmanager
    def getConnection(self):
        """
        Context manager for database connections.
        
        Yields:
            sqlite3.Connection: Database connection object
        """
        connection = None
        try:
            connection = sqlite3.connect(
                self.dbPath, 
                timeout=self.connectionTimeout
            )
            connection.row_factory = sqlite3.Row
            yield connection
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def initializeDatabase(self) -> bool:
        """
        Initializes database tables and sample data.
        
        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            with self.getConnection() as connection:
                cursor = connection.cursor()
                
                self._createUserTable(cursor)
                self._insertSampleData(cursor)
                
                connection.commit()
                logging.info("Database initialized successfully")
                return True
                
        except sqlite3.Error as e:
            logging.error(f"Database initialization failed: {e}")
            return False
    
    def _createUserTable(self, cursor) -> None:
        """Creates the users table if it doesn't exist."""
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                age INTEGER CHECK(age >= 0 AND age <= 150),
                department TEXT NOT NULL,
                salary REAL CHECK(salary >= 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
    def _insertSampleData(self, cursor) -> None:
        """Inserts sample user data into the database."""
        sampleUsers = [
            (1, "John Doe", "john@example.com", 30, "IT", 75000.0),
            (2, "Jane Smith", "jane@example.com", 28, "HR", 65000.0),
            (3, "Bob Johnson", "bob@example.com", 35, "Finance", 80000.0),
            (4, "Alice Brown", "alice@example.com", 32, "IT", 78000.0),
            (5, "Charlie Davis", "charlie@example.com", 29, "Marketing", 70000.0)
        ]
        
        cursor.executemany('''
            INSERT OR REPLACE INTO users 
            (id, name, email, age, department, salary) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', sampleUsers)