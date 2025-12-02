"""
Python Insertion Script (db_operations.py)

This script loads the processed review data (reviews_final.csv) into 
a PostgreSQL database, creating two tables: 'banks' and 'reviews'.
It handles the foreign key relationship by inserting unique banks first.
"""
import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import sys
import time

# Add parent directory to path to allow importing modules from there (if necessary for config)
# This assumes config.py is one level up from db_operations.py if running from a script folder
try:
    from config import DATA_PATHS, DB_CONFIG, EXPECTED_COLUMNS, BANK_NAMES
except ImportError:
    # Fallback/Debug note if running from a different location
    print("FATAL: Could not import config.py. Check sys.path setup.")
    sys.exit(1)


def connect_to_db(config):
    """Establishes a connection to the PostgreSQL database with retries."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to PostgreSQL (Attempt {attempt + 1}/{max_retries})...")
            # Connect using parameters from DB_CONFIG
            conn = psycopg2.connect(
                host=config['HOST'],
                database=config['DATABASE'],
                user=config['USER'],
                password=config['PASSWORD'],
                port=config['PORT']
            )
            print("✓ Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
            # OperationalError covers failed connections, invalid credentials, database not found, etc.
            print(f"Connection failed: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("✗ Maximum connection attempts reached. Aborting.")
                return None
    return None


def create_tables(conn):
    """Creates the necessary tables (banks and reviews) if they don't exist."""
    print("\nStarting table creation...")
    
    # SQL to create the banks table (dimension table)
    CREATE_BANKS_TABLE = """
    CREATE TABLE IF NOT EXISTS banks (
        bank_id SERIAL PRIMARY KEY,
        bank_code VARCHAR(50) UNIQUE NOT NULL,
        bank_name VARCHAR(255) NOT NULL
    );
    """
    
    # SQL to create the reviews table (fact table)
    CREATE_REVIEWS_TABLE = """
    CREATE TABLE IF NOT EXISTS reviews (
        review_id SERIAL PRIMARY KEY,
        bank_id INT REFERENCES banks(bank_id) ON DELETE CASCADE,
        review_text TEXT NOT NULL,
        rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
        review_date DATE,
        sentiment_label VARCHAR(50),
        sentiment_score NUMERIC(5, 4) -- Score up to 4 decimal places (e.g., 0.9876)
    );
    """

    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_BANKS_TABLE)
            print("  - Table 'banks' ensured to exist.")
            cur.execute(CREATE_REVIEWS_TABLE)
            print("  - Table 'reviews' ensured to exist.")
            conn.commit()
    except Exception as e:
        print(f"✗ An error occurred during table creation: {e}")
        conn.rollback()
        # Re-raise the exception to stop the main process
        raise


def insert_banks(conn, bank_codes):
    """
    Inserts unique bank codes into the 'banks' table and returns a 
    bank_code to bank_id lookup map.
    """
    print("\nInserting unique banks into 'banks' table...")
    bank_id_lookup = {}
    
    # Prepare data for insertion (code and full name)
    # BANK_NAMES is imported from config.py
    bank_data = [(code, BANK_NAMES.get(code, code)) for code in bank_codes]
    
    # SQL to insert or update banks


    INSERT_BANK_SQL = """
    INSERT INTO banks (bank_code, bank_name)
    VALUES (%s, %s)
    ON CONFLICT (bank_code) DO NOTHING;
    """
    
    # SQL to fetch ALL existing banks and their IDs for the lookup map
    SELECT_BANKS_SQL = "SELECT bank_id, bank_code FROM banks;"
    
    try:
        with conn.cursor() as cur:
            # Using execute_batch for fast bank insertion
            psycopg2.extras.execute_batch(cur, INSERT_BANK_SQL, bank_data)
            
            # Fetch all bank IDs to create the lookup map
            cur.execute(SELECT_BANKS_SQL)
            results = cur.fetchall()
            
            for bank_id, bank_code in results:
                bank_id_lookup[bank_code] = bank_id
                
            conn.commit()
            print(f"✓ Banks inserted/verified. Found {len(bank_id_lookup)} unique banks.")
            return bank_id_lookup
            
    except Exception as e:
        print(f"✗ An error occurred during bank insertion: {e}")
        conn.rollback()
        raise


def insert_reviews(conn, df, bank_id_lookup):
    """
    Inserts all review records into the 'reviews' table.
    Uses 'execute_batch' for efficient bulk insertion and robust SQL handling.
    """
    print("\nInserting review records into 'reviews' table...")
    
    # 1. Prepare data rows for insertion
    data_to_insert = []
    
    for index, row in df.iterrows():
        bank_code = row['bank_code']
        # Map the bank_code from the CSV to the bank_id from the DB lookup
        bank_id = bank_id_lookup.get(bank_code)
        
        if bank_id is None:
            # This should ideally not happen if data is clean
            print(f"Warning: Skipping review for unknown bank code: {bank_code}")
            continue
            
        data_to_insert.append((
            bank_id,
            row['review_text'],
            int(row['rating']), 
            row['review_date'],
            row['sentiment_label'],
            float(row['sentiment_score'])
        ))

    # 2. Define the SQL command
    INSERT_REVIEW_SQL = """
    INSERT INTO reviews (
        bank_id, review_text, rating, review_date, sentiment_label, sentiment_score
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    -- Using ON CONFLICT DO NOTHING to ensure idempotent loads (no duplicate insertion)
    ON CONFLICT DO NOTHING; 
    """

    # 3. Execute bulk insertion using execute_batch
    try:
        with conn.cursor() as cur:
            # execute_batch handles the SQL template and list of tuples more reliably than execute_values here.
            psycopg2.extras.execute_batch(
                cur, INSERT_REVIEW_SQL, data_to_insert, page_size=100
            )
            inserted_count = cur.rowcount
            conn.commit()
            print(f"✓ Successfully processed and inserted {inserted_count} reviews.")
            
    except Exception as e:
        print(f"✗ An error occurred during review insertion: {e}")
        conn.rollback()
        raise


def main():
    """Main function to run the data insertion pipeline."""
    # --- 1. Load Data ---
    input_file = DATA_PATHS['final_results']
    if not os.path.exists(input_file):
        print(f"ERROR: Input file not found at {input_file}. Please run the preprocessing/sentiment steps first.")
        return

    try:
        df = pd.read_csv(input_file)
        
        # Validation check for required columns
        if not all(col in df.columns for col in EXPECTED_COLUMNS):
            missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
            print(f"ERROR: CSV is missing required columns: {missing}")
            return
            
        print(f"Successfully loaded {len(df)} records from {input_file}.")
        
    except Exception as e:
        print(f"ERROR: Failed to load CSV file: {e}")
        return

    conn = None
    try:
        # --- 2. Database Connection ---
        conn = connect_to_db(DB_CONFIG)
        if conn is None:
            return

        # --- 3. Create Tables ---
        create_tables(conn)

        # --- 4. Get Unique Banks and Insert/Lookup ---
        unique_bank_codes = df['bank_code'].unique()
        bank_id_lookup = insert_banks(conn, unique_bank_codes)
        
        if not bank_id_lookup:
            print("ERROR: Failed to retrieve bank ID lookup. Cannot insert reviews.")
            return

        # --- 5. Insert Reviews ---
        insert_reviews(conn, df, bank_id_lookup)
        
        print("\n=======================================================")
        print(f"SUCCESS: ETL Process Complete! {len(df)} records inserted.")
        print("=======================================================")
        
    except Exception as e:
        print(f"\nFATAL ERROR: Data pipeline failed. Review the error message and database connection/schema.")
        # Only exit with error code if connection succeeded but insertion failed
        if conn:
            sys.exit(1)
        
    finally:
        # --- 6. Close Connection ---
        if conn:
            conn.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()