"""
Data Preprocessing Script
Task 1: Data Preprocessing

This script cleans and preprocesses the scraped reviews data.
- Handles missing values
- Normalizes dates
- Cleans text data
- Removes duplicates
"""

# Import the sys module to handle system-specific parameters and functions
import sys
# Import the os module to handle file paths and operating system functionalities
import os

# Add the parent directory of the current script to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import pandas for data manipulation and analysis (DataFrames)
import pandas as pd
import numpy as np
from datetime import datetime
import re
# Import DATA_PATHS dictionary from the local config module
from config import DATA_PATHS


class ReviewPreprocessor:
    """Preprocessor class for review data"""

    def __init__(self, input_path=None, output_path=None):
        """
        Initialize preprocessor
        """
        self.input_path = input_path or DATA_PATHS['raw_reviews']
        self.output_path = output_path or DATA_PATHS['processed_reviews']
        self.df = None
        self.stats = {}

    def load_data(self):
        """Load raw reviews data"""
        print("Loading raw data...")
        try:
            self.df = pd.read_csv(self.input_path)
            print(f"Loaded {len(self.df)} reviews")
            self.stats['original_count'] = len(self.df)
            return True
        except FileNotFoundError:
            print(f"ERROR: File not found: {self.input_path}")
            return False
        except Exception as e:
            print(f"ERROR: Failed to load data: {str(e)}")
            return False

    def remove_duplicates(self):
        """Remove exact duplicate reviews"""
        print("\n[1/6] Removing duplicates...")
        initial_count = len(self.df)
        
        # Keep the first occurrence of duplicate review text + bank
        self.df.drop_duplicates(subset=['review_text', 'bank_code'], keep='first', inplace=True)
        
        removed = initial_count - len(self.df)
        self.stats['duplicates_removed'] = removed
        print(f"Removed {removed} duplicates. Remaining records: {len(self.df)}")


    def handle_missing_values(self):
        """Handle missing data (remove critical, fill non-critical)"""
        print("\n[2/6] Handling missing values...")
        
        initial_count = len(self.df)

        # 1. Remove rows where CRITICAL columns are missing (review_text, rating, review_date)
        critical_cols = ['review_text', 'rating', 'review_date']
        self.df.dropna(subset=critical_cols, inplace=True)
        
        # 2. Fill non-critical missing values
        # Fill missing 'reply_content' with an empty string
        self.df['reply_content'] = self.df['reply_content'].fillna('')
        # Fill missing 'app_id' with 'N/A'
        self.df['app_id'] = self.df['app_id'].fillna('N/A')

        removed = initial_count - len(self.df)
        self.stats['rows_removed_missing'] = removed
        self.stats['count_after_missing'] = len(self.df)
        print(f"Removed {removed} rows with missing critical data.")


    def normalize_dates(self):
        """Normalize date formats to YYYY-MM-DD"""
        print("\n[3/6] Normalizing dates...")
        try:
            # Convert the 'review_date' column to datetime objects
            self.df['date'] = pd.to_datetime(self.df['review_date'], utc=True).dt.date
            # Convert datetime objects back to the required string format YYYY-MM-DD
            self.df['date'] = self.df['date'].astype(str)
            print("Date normalization to YYYY-MM-DD completed.")
        except Exception as e:
            print(f"ERROR during date normalization: {str(e)}")


    def clean_text(self):
        """Clean the review text data (basic cleaning)"""
        
        print("\n[4/6] Cleaning text data...")
        # Function to apply basic text cleaning
        def basic_text_clean(text):
            if pd.isna(text):
                return text
            text = str(text).lower()
            text = re.sub(r'[\r\n]+', ' ', text) # Remove newlines/carriage returns
            text = re.sub(r'\s+', ' ', text).strip() # Replace multiple spaces with a single space
            return text

        # Apply cleaning to the review text
        self.df['review_text'] = self.df['review_text'].apply(basic_text_clean)
        self.df['reply_content'] = self.df['reply_content'].apply(basic_text_clean)
        print("Basic text cleaning completed (lowercasing, whitespace removal).")

    
    def validate_ratings(self):
        """Ensure rating values are within the 1-5 range and are integers"""
        print("\n[5/6] Validating ratings...")
        
        # Convert rating to integer, coercing errors to NaN
        self.df['rating'] = pd.to_numeric(self.df['rating'], errors='coerce', downcast='integer')
        
        # Remove rows where rating is missing or outside 1-5
        initial_count = len(self.df)
        self.df.dropna(subset=['rating'], inplace=True) # drop NaNs created by coercion
        self.df = self.df[(self.df['rating'] >= 1) & (self.df['rating'] <= 5)]
        
        removed = initial_count - len(self.df)
        self.stats['rows_removed_invalid_rating'] = removed
        print(f"Removed {removed} rows with invalid ratings. Remaining: {len(self.df)}")


    def prepare_final_output(self):
        """Select and rename columns for the final required CSV format"""
        print("\n[6/6] Preparing final output columns...")
        
        # Rename and select columns to match the required output: review, rating, date, bank, source
        self.df = self.df.rename(columns={
            'review_text': 'review',
            'bank_name': 'bank',
        })

        # Select the final columns in the required order
        self.df = self.df[[
            'review', 
            'rating', 
            'date', 
            'bank', 
            'source'
        ]].copy()
        
        # Ensure the final columns are of the correct type
        self.df['rating'] = self.df['rating'].astype(int)
        self.df['source'] = 'Google Play Store' # Ensure explicit source column
        print("Final output columns prepared: review, rating, date, bank, source.")


    def save_data(self):
        """Save the processed data to the output CSV file"""
        print("\nSaving processed data...")
        try:
            # Ensure the processed data directory exists
            os.makedirs(DATA_PATHS['processed'], exist_ok=True)
            self.df.to_csv(self.output_path, index=False, encoding='utf-8')
            self.stats['final_count'] = len(self.df)
            print(f"✓ Processed data saved successfully to: {self.output_path}")
            return True
        except Exception as e:
            print(f"ERROR: Failed to save data: {str(e)}")
            return False

    def generate_report(self):
        """Generate preprocessing report"""
        print("\n" + "=" * 60)
        print("PREPROCESSING REPORT")
        print("=" * 60)
        
        # Calculate missing data percentage based on final count
        missing_count = self.stats.get('rows_removed_missing', 0)
        total_removed = self.stats.get('duplicates_removed', 0) + missing_count + self.stats.get('rows_removed_invalid_rating', 0)
        
        # For KPI check: What percentage of the *final* dataset's required columns (review, rating, date) are missing?
        # Since we dropped rows with missing critical data, the missing data percentage in the *final* output is 0%.
        # We can calculate the total data loss from the raw scrape.
        
        original_count = self.stats.get('original_count', 0)
        final_count = self.stats.get('final_count', 0)
        data_loss_pct = 0
        if original_count > 0:
            data_loss_pct = (total_removed / original_count) * 100
        print(f"Original Records (Raw Scrape): {original_count}")
        print(f"Duplicates Removed: {self.stats.get('duplicates_removed', 0)}")
        print(f"Rows Removed (Missing Critical Data/Invalid Rating): {missing_count + self.stats.get('rows_removed_invalid_rating', 0)}")            print("-" * 60)
        print(f"Final Records (Cleaned): {final_count}")
        print(f"Total Data Loss from Original: {total_removed} ({data_loss_pct:.2f}%)")
        
        # KPI Check
        print("\n--- KPI CHECK ---")
        reviews_collected_kpi = "✓ Passed" if final_count >= 1200 else "✗ Failed"
        missing_data_kpi = "✓ Passed" if data_loss_pct < 5.0 else "✗ Failed"
        
        print(f"KPI 1: 1,200+ reviews collected: {reviews_collected_kpi} ({final_count} collected)")
        print(f"KPI 2: <5% Missing Data (Data Loss): {missing_data_kpi} ({data_loss_pct:.2f}%)")
        print("=" * 60)


    def process(self):
        """Run complete preprocessing pipeline"""
        print("\n" + "=" * 60)
        print("STARTING DATA PREPROCESSING")
        print("=" * 60)

        # Attempt to load data. If it fails, return False immediately.
        if not self.load_data():
            return False

        # Run each step of the pipeline in sequence
        self.remove_duplicates()
        self.handle_missing_values()
        self.normalize_dates()
        self.clean_text()
        self.validate_ratings()
        self.prepare_final_output()

        # Attempt to save the data. If successful, generate the report.
        if self.save_data():
            self.generate_report()
            return True

        # If saving failed, return False
        return False


def main():
    """Main execution function"""
    # Create an instance of the ReviewPreprocessor class
    preprocessor = ReviewPreprocessor()
    # Run the processing pipeline
    success = preprocessor.process()

    # Check if the process was successful
    if success:
        print("\n✓ Preprocessing completed successfully!")
    else:
        print("\n✗ Preprocessing failed!")


# Standard Python check to see if this file is being run directly (not imported)
if __name__ == "__main__":
    main()