# email_processor/utils/db_utils.py
import sqlite3
import logging
import pandas as pd

def setup_database(db_path: str):
    """
    Creates the target table in the SQLite database if it does not exist.
    In a real enterprise environment, this would be a connection to SQL Server or PostgreSQL.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS enterprise_data (
        reference_id TEXT PRIMARY KEY,
        group_name TEXT,
        vendor_name TEXT,
        target_date TEXT,
        primary_location TEXT,
        document_no TEXT,
        secondary_location TEXT,
        item_type TEXT,
        ingestion_timestamp TEXT,
        source_file TEXT
    );
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database initialization failed: {e}")

def upsert_vendor_data(df: pd.DataFrame, db_path: str) -> bool:
    """
    Saves the extracted DataFrame into the database.
    Uses an 'Upsert' logic (Insert or Replace) via a staging table 
    to avoid duplicate entries and ensure idempotency.
    """
    if df is None or df.empty:
        logging.warning("No data provided for database insertion.")
        return False

    # Ensure the main table exists before trying to insert
    setup_database(db_path)

    try:
        with sqlite3.connect(db_path) as conn:
            # Step 1: Load data into a temporary staging table
            df.to_sql('staging_table', conn, if_exists='replace', index=False)
            
            # Step 2: Merge staging data into the main table (Upsert logic)
            # If the reference_id already exists, it updates the row. If not, it inserts.
            upsert_query = """
            INSERT OR REPLACE INTO enterprise_data (
                reference_id, group_name, vendor_name, target_date, primary_location, 
                document_no, secondary_location, item_type, ingestion_timestamp, source_file
            )
            SELECT 
                reference_id, group_name, vendor_name, target_date, primary_location, 
                document_no, secondary_location, item_type, ingestion_timestamp, source_file
            FROM staging_table;
            """
            conn.execute(upsert_query)
            
            # Step 3: Clean up the staging table
            conn.execute("DROP TABLE staging_table;")
            
            logging.info(f"Successfully upserted {len(df)} rows into the database.")
            return True

    except Exception as e:
        logging.error(f"Database upsert failed: {e}")
        return False