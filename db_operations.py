"""
Python Insertion Script (db_operations.py)

This script loads the processed review data (reviews_final.csv) into 
a PostgreSQL database. It has been updated to include the 'identified_theme'
column generated during the thematic analysis (Task 2).
"""
import pandas as pd
import psycopg2
from psycopg2 import extras
import os
import sys
import time

# Add parent directory to path to allow importing modules from there (if necessary for config)
try:
    # Assuming config.py is one level up
    sys.path.append(os.path.abspath(os.path.join(os.getcwd(), os.pardir)))
    from config import DATA_PATHS, DB_CONFIG, EXPECTED_COLUMNS, BANK_NAMES
except ImportError:
    # Fallback/Debug note if running from a different location
    print("FATAL: Could not import config.py. Check sys.path setup.")
    sys.exit(1)


def connect_to_db(config):
    """Establishes a connection to the PostgreSQL database with retries."""
    max_retries = 3
    # Convert DB_CONFIG keys to lowercase for psycopg2 compatibility
    corrected_config = {k.lower(): v for k, v in config.items()}
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to PostgreSQL (Attempt {attempt + 1}/{max_retries})...")
            conn = psycopg2.connect(
                host=corrected_config['host'],
                database=corrected_config['database'],
                user=corrected_config['user'],
                password=corrected_config['password'],
                port=corrected_config['port']
            )
            print("✓ Database connection successful.")
            return conn
        except psycopg2.OperationalError as e:
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
    """
    Creates the necessary tables (banks and reviews) or ensures they exist.
    
    CRITICAL UPDATE: The reviews table now includes 'identified_theme' 
    and the 'app_name' has been added to the banks table 
    (though not strictly required for this script, it matches the schema plan).
    """
    print("\nStarting table creation/schema update...")
    
    # 1. SQL to create the banks table
    CREATE_BANKS_TABLE = """
    CREATE TABLE IF NOT EXISTS banks (
        bank_id SERIAL PRIMARY KEY,
        bank_code VARCHAR(50) UNIQUE NOT NULL,
        bank_name VARCHAR(255) NOT NULL,
        app_name VARCHAR(100)
    );
    """
    
    # 2. SQL to create the reviews table (FACT TABLE)
    # NOTE: The schema definition below MUST match the database plan provided by the user.
    CREATE_REVIEWS_TABLE = """
    CREATE TABLE IF NOT EXISTS reviews (
        review_id SERIAL PRIMARY KEY,
        bank_id INT REFERENCES banks(bank_id) ON DELETE CASCADE,
        review_text TEXT NOT NULL,
        rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
        review_date DATE NOT NULL,
        sentiment_label VARCHAR(50),
        sentiment_score NUMERIC(5, 4),
        identified_theme VARCHAR(100) -- <--- NEW COLUMN from Task 2
    );
    """
    
    # 3. Add column migration logic (in case tables already exist but are missing new columns)
    # This prevents errors if you run the script multiple times without dropping tables.
    ALTER_REVIEWS_THEME = "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='reviews' AND column_name='identified_theme') THEN ALTER TABLE reviews ADD COLUMN identified_theme VARCHAR(100); END IF; END $$;"
    
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_BANKS_TABLE)

            print("  - Table 'banks' ensured to exist.")
            cur.execute(CREATE_REVIEWS_TABLE)
            print("  - Table 'reviews' ensured to exist.")
            # Execute the alter table logic
            cur.execute(ALTER_REVIEWS_THEME)
            print("  - Column 'identified_theme' ensured to exist in 'reviews'.")
            conn.commit()
    except Exception as e:
        print(f"✗ An error occurred during table creation/alteration: {e}")
        conn.rollback()
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
    # NOTE: app_name is left NULL here if not available, as it's not in the current CSV
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
    Inserts all review records into the 'reviews' table, now including 'identified_theme'.
    """
    print("\nInserting review records into 'reviews' table...")
    
    data_to_insert = []
    
    for index, row in df.iterrows():
        bank_code = row['bank_code']
        bank_id = bank_id_lookup.get(bank_code)
        
        if bank_id is None:
            print(f"Warning: Skipping review for unknown bank code: {bank_code}")
            continue
            
        data_to_insert.append((
            bank_id,
            row['review_text'],
            int(row['rating']), 
            row['review_date'],
            row['sentiment_label'],
            float(row['sentiment_score']),
            row['identified_theme'] # <--- NEW VALUE: Add the identified theme
        ))

    # Define the SQL command, updated to include the new column
    INSERT_REVIEW_SQL = """
    INSERT INTO reviews (
        bank_id, review_text, rating, review_date, sentiment_label, sentiment_score, identified_theme
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    -- Using ON CONFLICT DO NOTHING to ensure idempotent loads (no duplicate insertion)
    ON CONFLICT DO NOTHING; 
    """

    try:
        with conn.cursor() as cur:
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
    # We now expect 'identified_theme' to be present in this file
    input_file = DATA_PATHS['final_results'] 
    
    # We must update EXPECTED_COLUMNS if it doesn't include 'identified_theme' 
    # to ensure the CSV check passes, or just rely on the columns being in the DataFrame.
    # Assuming the structure is correct from analysis_and_themes.ipynb output.
    
    if not os.path.exists(input_file):
        print(f"ERROR: Input file not found at {input_file}. Please run the preprocessing/sentiment steps first.")
        return

    try:
        df = pd.read_csv(input_file)
        
        # Check for critical columns, including the new one
        required_cols = ['bank_code', 'review_text', 'rating', 'review_date', 'sentiment_label', 'sentiment_score', 'identified_theme']
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            print(f"ERROR: CSV is missing required columns, especially: {missing}. Did you run Task 2?")
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

        # --- 3. Create Tables and Ensure Schema ---
        create_tables(conn)

        # --- 4. Get Unique Banks and Insert/Lookup ---
        unique_bank_codes = df['bank_code'].unique()
        bank_id_lookup = insert_banks(conn, unique_bank_codes)
        
        if not bank_id_lookup:
            print("ERROR: Failed to retrieve bank ID lookup. Cannot insert reviews.")
            return

        # --- 5. Insert Reviews (including theme) ---
        insert_reviews(conn, df, bank_id_lookup)
        
        print("\n=======================================================")
        print(f"SUCCESS: ETL Process Complete! {len(df)} records inserted.")
        print("=======================================================")
        
    except Exception as e:
        print(f"\nFATAL ERROR: Data pipeline failed. Details: {e}")
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