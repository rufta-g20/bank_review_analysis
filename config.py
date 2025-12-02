"""
Configuration file for Bank Reviews Analysis Project
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Google Play Store App IDs (CBE, BOA, Dashen)
APP_IDS = {
    # Commercial Bank of Ethiopia
    'CBE': os.getenv('CBE_APP_ID', 'com.combanketh.mobilebanking'),
    # Bank of Abyssinia (BOA) - Using their official app ID
    'BOA': os.getenv('BOA_APP_ID', 'com.boa.boaMobileBanking'),
    # Dashen Bank - Using their SuperApp ID
    'Dashen': os.getenv('DASHEN_APP_ID', 'com.dashen.dashensuperapp')
}

# Bank Names Mapping
BANK_NAMES = {
    'CBE': 'Commercial Bank of Ethiopia',
    'BOA': 'Bank of Abyssinia',
    'Dashen': 'Dashen Bank'
}

# Scraping Configuration
SCRAPING_CONFIG = {
    'reviews_per_bank': int(os.getenv('REVIEWS_PER_BANK', 600)),
    'max_retries': int(os.getenv('MAX_RETRIES', 3)),
    'lang': 'en',
    'country': 'et'  # Ethiopia
}

# File Paths
DATA_PATHS = {
    'raw': 'data/raw',
    'processed': 'data/processed',
    'raw_reviews': 'data/raw/reviews_raw.csv',
    'processed_reviews': 'data/processed/reviews_processed.csv',
    'sentiment_results': 'data/processed/reviews_with_sentiment.csv',
    'final_results': 'data/processed/reviews_final.csv'
}


# Note: These values should be set in your project's .env file
DB_CONFIG = {
    'HOST': os.getenv('PG_HOST', 'localhost'),
    'DATABASE': os.getenv('PG_DATABASE', 'bank_reviews'),
    'USER': os.getenv('PG_USER', 'postgres_user'),
    'PASSWORD': os.getenv('PG_PASSWORD', 'your_secure_password'),
    'PORT': os.getenv('PG_PORT', 5432)
}

# List of columns expected in the final CSV file for validation
EXPECTED_COLUMNS = [
    'bank_code', 'review_text', 'rating', 'review_date', 
    'sentiment_label', 'sentiment_score'
]