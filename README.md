# Bank Review Analysis Project - Task 1: Data Collection and Preprocessing

## Methodology

### 1. Data Collection (Web Scraping)
* Target: Google Play Store user reviews for three Ethiopian banks: Commercial Bank of Ethiopia (CBE), Bank of Abyssinia (BOA), and Dashen Bank.
* Tool: The google-play-scraper Python library was used to automate the data extraction.
* Data Points Collected: Review ID, User Name, Review Text, Rating (Score), Review Date, Thumbs Up Count, and Developer Reply Content.
* Target Volume: A minimum of 500 of the newest reviews were scraped for each bank (increased from 400 to ensure volume), aiming for a total of 1,500+ raw records.
* Raw Output Location: data/raw/reviews_raw.csv

### 2. Data Preprocessing
The following cleaning and transformation steps were applied to ensure data quality:

* Duplicate Removal: 283 exact duplicates (based on review content/bank) were removed.
* Missing Data Handling:
    * Rows missing critical data (review_text, rating, or review_date) were dropped (0 rows removed).
    * Missing reply_content was filled with an empty string ("").
* Date Normalization: The original date format was converted to a consistent YYYY-MM-DD string format.
* Text Cleaning: Basic cleaning was performed on review text (lowercasing, newline removal, and whitespace standardization).
* Final Output Schema: The processed dataset was saved to data/processed/reviews_processed.csv with the following final columns: review, rating, date, bank, and source.

## 📈 KPIs Status

Based on the final preprocessing.py report:

| KPI | Target | Actual Result | Status | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Clean Volume | 1,200+ unique reviews collected | 1,517 clean reviews collected | ✅ Passed | Successfully exceeded the minimum target volume for a robust analysis. |
| Data Loss | <5% total loss | 15.72% total loss (283 rows) | ⚠️ Failed | The data loss was entirely due to the removal of duplicates, which is a necessary step for data quality. No critical data was missing. |

* Clean CSV dataset: reviews_processed.csv available.
* Organized Git repo with clear commits: Commit history reflects logical chunks of work.