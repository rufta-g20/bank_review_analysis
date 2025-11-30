"""
Google Play Store Review Scraper
Task 1: Data Collection

This script scrapes user reviews from Google Play Store for three Ethiopian banks.
Target: 400+ reviews per bank (1200 total minimum)
"""

import sys
import os
# Add parent directory to path to allow importing modules from there (if running from a sub-directory)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google_play_scraper import app, Sort, reviews_all, reviews
import pandas as pd
from datetime import datetime
import time
from tqdm import tqdm
# Import configuration from config.py
from config import APP_IDS, BANK_NAMES, SCRAPING_CONFIG, DATA_PATHS


class PlayStoreScraper:
    """Scraper class for Google Play Store reviews"""

    def __init__(self):
        # Load configuration variables from the config file
        self.app_ids = APP_IDS
        self.bank_names = BANK_NAMES
        self.reviews_per_bank = SCRAPING_CONFIG['reviews_per_bank']
        self.lang = SCRAPING_CONFIG['lang']
        self.country = SCRAPING_CONFIG['country']
        self.max_retries = SCRAPING_CONFIG['max_retries']
        self.all_reviews = []

    def _format_review(self, review, bank_code):
        """Helper to format a single review into a flat dictionary"""
        return {
            'review_id': review.get('reviewId'),
            'user_name': review.get('userName'),
            'review_text': review.get('content'),
            'rating': review.get('score'),
            'review_date': review.get('at'),
            'thumbs_up_count': review.get('thumbsUpCount', 0),
            'reply_content': review.get('replyContent', None),
            'bank_code': bank_code,
            'bank_name': self.bank_names[bank_code],
            'app_id': review.get('appVersion', 'N/A'),
            'source': 'Google Play Store'
        }

    def scrape_reviews(self):
        """
        Scrape reviews for all configured apps.
        """
        print("=" * 60)
        print("STARTING GOOGLE PLAY STORE SCRAPING")
        print("=" * 60)
        
        start_time = time.time()
        
        for bank_code, app_id in self.app_ids.items():
            print(f"\n--- Scraping {self.bank_names[bank_code]} ({app_id}) ---")
            
            # Use reviews_all for maximum reviews, but cap the number we process
            # reviews_all is less flexible with limits, but handles pagination better
            # We'll rely on the default Sort.NEWEST or Sort.MOST_RELEVANT
            
            scraped_reviews = []
            retries = 0

            while retries < self.max_retries:
                try:
                    # Fetching the maximum number of reviews available up to a very high limit
                    # The library fetches batches, so we let it run and then slice
                    full_review_list = reviews_all(
                        app_id,
                        lang=self.lang,
                        country=self.country,
                        sort=Sort.NEWEST # Get the most recent reviews
                    )
                    
                    # Take up to the requested number of reviews per bank
                    scraped_reviews = full_review_list[:self.reviews_per_bank]
                    print(f"  Successfully collected {len(scraped_reviews)} reviews.")
                    break # Exit retry loop on success

                except Exception as e:
                    retries += 1
                    print(f"  ERROR: Scrape failed for {bank_code}. Retrying in 5 seconds... (Attempt {retries}/{self.max_retries})")
                    print(f"  Details: {str(e)}")
                    if retries == self.max_retries:
                        print(f"  Max retries reached. Skipping {bank_code}.")
                        break
                    time.sleep(5)

# Format and add to the main list
            if scraped_reviews:
                formatted_reviews = [self._format_review(review, bank_code) for review in tqdm(scraped_reviews, desc=f"  Formatting {bank_code} Reviews")]
                self.all_reviews.extend(formatted_reviews)

        end_time = time.time()
        print(f"\nTotal scraping time: {end_time - start_time:.2f} seconds")
        print(f"Total reviews collected: {len(self.all_reviews)}")
        
        return self._save_data()

    def _save_data(self):
        """
        Convert list of reviews to DataFrame and save to CSV.
        """
        if self.all_reviews:
            df = pd.DataFrame(self.all_reviews)

            # Ensure the raw data directory exists
            os.makedirs(DATA_PATHS['raw'], exist_ok=True)
            df.to_csv(DATA_PATHS['raw_reviews'], index=False, encoding='utf-8')
            
            print("\n--- Scraping Report ---")
            print(f"Total reviews collected: {len(df)}")
            print("Reviews collected per bank:")
            for bank_code in self.bank_names.keys():
                count = len(df[df['bank_code'] == bank_code])
                print(f"  {self.bank_names[bank_code]}: {count}")

            print(f"\nRaw data saved to: {DATA_PATHS['raw_reviews']}")

            return df
        else:
            print("\nERROR: No reviews were collected!")
            return pd.DataFrame()


def main():
    """Main execution function"""
    # Initialize scraper
    scraper = PlayStoreScraper()
    # Run scraping process
    df_raw = scraper.scrape_reviews()
    
    if not df_raw.empty:
        pass


# Standard Python check to see if this file is being run directly (not imported)
if __name__ == "__main__":
    main()