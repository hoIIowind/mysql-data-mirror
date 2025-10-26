#!/usr/bin/env python3
import os
import sys
import logging
import re
from typing import Dict, List, Tuple, Union
from datetime import datetime, timezone, timedelta
import mysql.connector
from mysql.connector import Error

# Load environment variables from .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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

BATCH_SIZE = 500  # Rows per batch

def ist_now() -> datetime:
    """Return current IST datetime as timezone-aware object"""
    utc_now = datetime.now(timezone.utc)          # UTC now, aware
    ist_offset = timedelta(hours=5, minutes=30)  # IST offset
    return utc_now + ist_offset

class DatabaseMirror:
    def __init__(self):
        self.source_config = {
            "host": os.getenv("SOURCE_DB_HOST"),
            "port": int(os.getenv("SOURCE_DB_PORT", 3306)),
            "user": os.getenv("SOURCE_DB_USER"),
            "password": os.getenv("SOURCE_DB_PASSWORD"),
            "database": os.getenv("SOURCE_DB_NAME"),
            "connection_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", 10)),
            "ssl_disabled": False
        }

        self.target_config = {
            "host": os.getenv("TARGET_DB_HOST"),
            "port": int(os.getenv("TARGET_DB_PORT", 3306)),
            "user": os.getenv("TARGET_DB_USER"),
            "password": os.getenv("TARGET_DB_PASSWORD", ""),
            "database": os.getenv("TARGET_DB_NAME"),
            "connection_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", 10)),
            "ssl_disabled": False
        }

        self.table_name = os.getenv("TABLE_NAME")
        self.validate_env()

    def validate_env(self):
        required_vars = [
            'SOURCE_DB_HOST', 'SOURCE_DB_USER', 'SOURCE_DB_PASSWORD', 'SOURCE_DB_NAME',
            'TARGET_DB_HOST', 'TARGET_DB_USER', 'TARGET_DB_NAME', 'TABLE_NAME'
        ]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    def get_connection(self, to_target: bool = False) -> mysql.connector.MySQLConnection:
        cfg = self.target_config if to_target else self.source_config
        retries = 3
        for i in range(retries):
            try:
                conn = mysql.connector.connect(**cfg)
                if conn.is_connected():
                    logger.info(f"Connected to {'target' if to_target else 'source'} database")
                    return conn
            except Error as e:
                logger.warning(f"Connection attempt {i+1} failed: {e}")
        raise ConnectionError(f"Unable to connect to {'target' if to_target else 'source'} database after {retries} attempts")

    def get_primary_keys(self, conn: mysql.connector.MySQLConnection) -> List[str]:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{conn.database}'
              AND TABLE_NAME = '{self.table_name}'
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """)
        pk_columns = [row['COLUMN_NAME'] for row in cur.fetchall()]
        cur.close()
        if not pk_columns:
            raise ValueError(f"No primary key defined for table {self.table_name}")
        return pk_columns

    def get_columns(self, conn: mysql.connector.MySQLConnection) -> List[str]:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"DESCRIBE {self.table_name}")
        columns = [row['Field'] for row in cur.fetchall()]
        cur.close()
        return columns

    def create_target_table_if_missing(self, src_conn, tgt_conn):
        tgt_cur = tgt_conn.cursor()
        try:
            tgt_cur.execute(f"SHOW TABLES LIKE '{self.table_name}'")
            if tgt_cur.fetchone():
                logger.info(f"Target table {self.table_name} already exists, skipping creation")
                return

            src_cur = src_conn.cursor(dictionary=True)
            src_cur.execute(f"SHOW CREATE TABLE `{self.table_name}`")
            row = src_cur.fetchone()
            create_stmt = row['Create Table']

            # Remove FOREIGN KEY constraints
            create_stmt = re.sub(
                r',?\s*CONSTRAINT `.*?` FOREIGN KEY .*?\)',
                '',
                create_stmt,
                flags=re.DOTALL
            )

            # Remove existing last_updated column if exists
            create_stmt = re.sub(
                r'`last_updated`\s+TIMESTAMP.*?,?',
                '',
                create_stmt,
                flags=re.DOTALL
            )

            # Add operation_type and last_updated (DATETIME)
            if 'operation_type' not in create_stmt:
                create_stmt = create_stmt.rstrip(')') + ",\n  `operation_type` VARCHAR(10) DEFAULT 'inserted',\n  `last_updated` DATETIME DEFAULT CURRENT_TIMESTAMP\n)"

            tgt_cur.execute("SET FOREIGN_KEY_CHECKS=0;")
            tgt_cur.execute(create_stmt)
            tgt_cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            tgt_conn.commit()
            logger.info(f"Target table {self.table_name} created successfully")
        finally:
            tgt_cur.close()
            if 'src_cur' in locals():
                src_cur.close()

    def fetch_table_data(self, conn, columns: List[str]) -> Dict[Union[str, Tuple], Tuple]:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"SELECT * FROM {self.table_name}")
        rows = cur.fetchall()
        cur.close()

        pk_cols = self.get_primary_keys(conn)
        data = {}
        for row in rows:
            pk_vals = tuple(row[pk] for pk in pk_cols)
            key = pk_vals[0] if len(pk_vals) == 1 else pk_vals
            data[key] = tuple(row[col] for col in columns)
        return data

    def sync_data(self):
        src_conn = tgt_conn = None
        try:
            src_conn = self.get_connection(to_target=False)
            tgt_conn = self.get_connection(to_target=True)
            tgt_cur = tgt_conn.cursor()

            self.create_target_table_if_missing(src_conn, tgt_conn)

            columns = self.get_columns(src_conn)
            columns = [c for c in columns if c not in ('operation_type', 'last_updated')]

            src_data = self.fetch_table_data(src_conn, columns)
            tgt_data = self.fetch_table_data(tgt_conn, columns)
            pk_cols = self.get_primary_keys(src_conn)

            inserted = updated = deleted = 0
            batch_inserts = []
            batch_updates = []

            tgt_cur.execute("SET FOREIGN_KEY_CHECKS=0;")

            # Inserts / Updates
            for key, src_row in src_data.items():
                if key not in tgt_data:
                    batch_inserts.append(src_row)
                    if len(batch_inserts) >= BATCH_SIZE:
                        self._execute_batch_insert(tgt_cur, columns, batch_inserts)
                        inserted += len(batch_inserts)
                        batch_inserts.clear()
                elif src_row != tgt_data[key]:
                    batch_updates.append((src_row, key))
                    if len(batch_updates) >= BATCH_SIZE:
                        updated += self._execute_batch_update(tgt_cur, columns, pk_cols, batch_updates)
                        batch_updates.clear()

            if batch_inserts:
                self._execute_batch_insert(tgt_cur, columns, batch_inserts)
                inserted += len(batch_inserts)
            if batch_updates:
                updated += self._execute_batch_update(tgt_cur, columns, pk_cols, batch_updates)

            # Deletes
            for key in tgt_data:
                if key not in src_data:
                    where_clause = " AND ".join(f"`{pk}`=%s" for pk in pk_cols)
                    tgt_cur.execute(
                        f"UPDATE {self.table_name} SET operation_type='deleted', last_updated=%s WHERE {where_clause}",
                        (ist_now(),) + (key if isinstance(key, tuple) else (key,))
                    )
                    deleted += 1

            tgt_conn.commit()
            tgt_cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            tgt_cur.close()

            logger.info(f"Synchronization complete: {inserted} inserted, {updated} updated, {deleted} deleted")
        finally:
            if src_conn and src_conn.is_connected():
                src_conn.close()
            if tgt_conn and tgt_conn.is_connected():
                tgt_conn.close()

    def _execute_batch_insert(self, cur, columns, batch):
        columns_str = ','.join(f"`{col}`" for col in columns)
        query = f"INSERT INTO {self.table_name} ({columns_str}, operation_type, last_updated) VALUES ({','.join(['%s']*len(columns))}, 'inserted', %s)"
        cur.executemany(query, [row + (ist_now(),) for row in batch])

    def _execute_batch_update(self, cur, columns, pk_cols, batch):
        count = 0
        for src_row, key in batch:
            set_clause = ','.join(f"`{col}`=%s" for col in columns)
            where_clause = ' AND '.join(f"`{pk}`=%s" for pk in pk_cols)
            query = f"UPDATE {self.table_name} SET {set_clause}, operation_type='updated', last_updated=%s WHERE {where_clause}"
            pk_values = key if isinstance(key, tuple) else (key,)
            cur.execute(query, src_row + (ist_now(),) + pk_values)
            count += 1
        return count


def main():
    try:
        mirror = DatabaseMirror()
        mirror.sync_data()
        logger.info("Database mirroring completed successfully")
    except Exception as e:
        logger.error(f"Mirroring failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()