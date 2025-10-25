#!/usr/bin/env python3
import os
import sys
import logging
import hashlib
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import mysql.connector
from mysql.connector import Error

# Load environment variables from .env file for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, use system environment variables

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('db_mirror.log')
    ]
)
logger = logging.getLogger(__name__)

class DatabaseMirror:
    def __init__(self):
        self.source_config = {
            'host': os.getenv('SOURCE_DB_HOST'),
            'port': int(os.getenv('SOURCE_DB_PORT', 3306)),
            'user': os.getenv('SOURCE_DB_USER'),
            'password': os.getenv('SOURCE_DB_PASSWORD'),
            'database': os.getenv('SOURCE_DB_NAME')
        }
        
        self.target_config = {
            'host': os.getenv('TARGET_DB_HOST'),
            'port': int(os.getenv('TARGET_DB_PORT', 3306)),
            'user': os.getenv('TARGET_DB_USER'),
            'password': os.getenv('TARGET_DB_PASSWORD', ''),
            'database': os.getenv('TARGET_DB_NAME')
        }
        
        self.table_name = os.getenv('TABLE_NAME')
        self.validate_config()
    
    def validate_config(self):
        """Validate all required environment variables are set"""
        required_vars = [
            'SOURCE_DB_HOST', 'SOURCE_DB_USER', 'SOURCE_DB_PASSWORD', 'SOURCE_DB_NAME',
            'TARGET_DB_HOST', 'TARGET_DB_USER', 'TARGET_DB_NAME', 'TABLE_NAME'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        # Check if TARGET_DB_PASSWORD exists (can be empty)
        if os.getenv('TARGET_DB_PASSWORD') is None:
            missing_vars.append('TARGET_DB_PASSWORD')
            
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    def get_connection(self, config: Dict) -> mysql.connector.MySQLConnection:
        """Create database connection with retry logic"""
        try:
            connection = mysql.connector.connect(**config)
            if connection.is_connected():
                return connection
        except Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def get_table_structure(self, connection: mysql.connector.MySQLConnection) -> List[str]:
        """Get table column names"""
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {self.table_name}")
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return columns
    
    def create_target_table_if_not_exists(self, source_conn: mysql.connector.MySQLConnection, 
                                        target_conn: mysql.connector.MySQLConnection):
        """Create target table with operation_type column if it doesn't exist"""
        cursor_source = source_conn.cursor()
        cursor_target = target_conn.cursor()
        
        try:
            # Get source table structure
            cursor_source.execute(f"SHOW CREATE TABLE `{self.table_name}`")
            create_statement = cursor_source.fetchone()[1]

            
            # Convert double quotes to backticks for MySQL compatibility
            create_statement = create_statement.replace('"', '`')
            
            # Replace table name with IF NOT EXISTS
            create_statement = create_statement.replace(
                f"CREATE TABLE `{self.table_name}`",
                f"CREATE TABLE IF NOT EXISTS `{self.table_name}`"
            )
            
            # Find the position to insert new columns
            
            # Find the last column definition and add our columns before the closing parenthesis
            pattern = r'(.*)(\s*\)\s*(ENGINE.*)?$)'
            match = re.match(pattern, create_statement, re.DOTALL)
            
            if match:
                before_close = match.group(1)
                after_close = match.group(2)
                
                # Add our columns
                create_statement = before_close + ",\n  `operation_type` VARCHAR(10) DEFAULT 'inserted',\n  `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" + after_close
            

            cursor_target.execute(create_statement)
            target_conn.commit()
            logger.info(f"Target table {self.table_name} created/verified")
            
        except Error as e:
            logger.error(f"Error creating target table: {e}")
            raise
        finally:
            cursor_source.close()
            cursor_target.close()
    
    def get_row_hash(self, row_data: Tuple) -> str:
        """Generate hash for row data to detect changes"""
        row_str = '|'.join(str(item) if item is not None else 'NULL' for item in row_data)
        return hashlib.md5(row_str.encode()).hexdigest()
    
    def fetch_source_data(self, connection: mysql.connector.MySQLConnection) -> Dict[str, Tuple]:
        """Fetch all data from source table with primary key mapping"""
        cursor = connection.cursor()
        
        # Get primary key column
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = '{self.source_config["database"]}' 
            AND TABLE_NAME = '{self.table_name}' 
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """)
        
        pk_columns = [row[0] for row in cursor.fetchall()]
        if not pk_columns:
            raise ValueError(f"No primary key found for table {self.table_name}")
        
        # Fetch all data
        cursor.execute(f"SELECT * FROM {self.table_name}")
        columns = [desc[0] for desc in cursor.description]
        
        data = {}
        for row in cursor.fetchall():
            # Create composite key for multiple primary keys
            pk_values = tuple(row[columns.index(pk)] for pk in pk_columns)
            pk_key = pk_values[0] if len(pk_values) == 1 else pk_values
            data[pk_key] = row
        
        cursor.close()
        logger.info(f"Fetched {len(data)} rows from source table")
        return data, columns, pk_columns
    
    def fetch_target_data(self, connection: mysql.connector.MySQLConnection, 
                         columns: List[str]) -> Dict[str, Tuple]:
        """Fetch existing data from target table"""
        cursor = connection.cursor()
        
        # Get primary key columns from target
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = '{self.target_config["database"]}' 
            AND TABLE_NAME = '{self.table_name}' 
            AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """)
        
        pk_columns = [row[0] for row in cursor.fetchall()]
        
        # Select only columns that exist in source (excluding operation_type and last_updated)
        source_columns = [col for col in columns if col not in ['operation_type', 'last_updated']]
        columns_str = ', '.join(f"`{col}`" for col in source_columns + pk_columns)
        
        cursor.execute(f"SELECT {columns_str} FROM {self.table_name} WHERE operation_type != 'deleted'")
        
        data = {}
        for row in cursor.fetchall():
            # Create composite key
            pk_values = tuple(row[len(source_columns) + i] for i in range(len(pk_columns)))
            pk_key = pk_values[0] if len(pk_values) == 1 else pk_values
            data[pk_key] = row[:len(source_columns)]  # Only source columns
        
        cursor.close()
        return data, pk_columns
    
    def sync_data(self):
        """Main synchronization logic"""
        source_conn = None
        target_conn = None
        
        try:
            logger.info("Starting database synchronization")
            
            # Establish connections
            source_conn = self.get_connection(self.source_config)
            target_conn = self.get_connection(self.target_config)
            
            # Create target table if needed
            self.create_target_table_if_not_exists(source_conn, target_conn)
            
            # Fetch data
            source_data, source_columns, pk_columns = self.fetch_source_data(source_conn)
            target_data, _ = self.fetch_target_data(target_conn, source_columns)
            
            cursor = target_conn.cursor()
            
            # Track operations
            inserted_count = 0
            updated_count = 0
            deleted_count = 0
            
            # Process source data (inserts and updates)
            for pk_key, source_row in source_data.items():
                if pk_key not in target_data:
                    # Insert new row
                    placeholders = ', '.join(['%s'] * len(source_row))
                    columns_str = ', '.join(f"`{col}`" for col in source_columns)
                    
                    query = f"""
                        INSERT INTO {self.table_name} ({columns_str}, operation_type) 
                        VALUES ({placeholders}, 'inserted')
                    """
                    cursor.execute(query, source_row)
                    inserted_count += 1
                    
                elif source_row != target_data[pk_key]:
                    # Update existing row
                    set_clause = ', '.join(f"`{col}` = %s" for col in source_columns)
                    where_clause = ' AND '.join(f"`{pk}` = %s" for pk in pk_columns)
                    
                    query = f"""
                        UPDATE {self.table_name} 
                        SET {set_clause}, operation_type = 'updated', last_updated = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    
                    pk_values = pk_key if isinstance(pk_key, tuple) else (pk_key,)
                    cursor.execute(query, source_row + pk_values)
                    updated_count += 1
            
            # Mark deleted rows
            for pk_key in target_data:
                if pk_key not in source_data:
                    where_clause = ' AND '.join(f"`{pk}` = %s" for pk in pk_columns)
                    query = f"""
                        UPDATE {self.table_name} 
                        SET operation_type = 'deleted', last_updated = CURRENT_TIMESTAMP
                        WHERE {where_clause}
                    """
                    pk_values = pk_key if isinstance(pk_key, tuple) else (pk_key,)
                    cursor.execute(query, pk_values)
                    deleted_count += 1
            
            target_conn.commit()
            cursor.close()
            
            logger.info(f"Synchronization completed: {inserted_count} inserted, {updated_count} updated, {deleted_count} deleted")
            
        except Exception as e:
            logger.error(f"Synchronization failed: {e}")
            if target_conn:
                target_conn.rollback()
            raise
            
        finally:
            if source_conn and source_conn.is_connected():
                source_conn.close()
            if target_conn and target_conn.is_connected():
                target_conn.close()

def main():
    try:
        mirror = DatabaseMirror()
        mirror.sync_data()
        logger.info("Database mirroring completed successfully")
    except Exception as e:
        logger.error(f"Database mirroring failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()