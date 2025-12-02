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

---

## Task 3: Data Storage (PostgreSQL Schema)

The cleaned, processed, and sentiment-analyzed data was loaded into a PostgreSQL database named bank_reviews using a Python script (db_operations.py) and the psycopg2 library. The database uses a normalized schema consisting of two tables.

### banks (Dimension Table)

This table stores a unique list of banks and serves as the primary key for foreign key relationships.

| Column Name | Data Type | Constraint / Description |
| :--- | :--- | :--- |
| bank\_id | SERIAL | PRIMARY KEY. Auto-incrementing unique identifier. |
| bank\_code | VARCHAR(50) | Unique identifier for the bank (e.g., 'CBE', 'BOA'). |
| bank\_name | VARCHAR(255) | Full name of the bank (e.g., 'Commercial Bank of Ethiopia'). |

### reviews (Fact Table)

This table stores the core review data, linking back to the banks table.

| Column Name | Data Type | Constraint / Description |
| :--- | :--- | :--- |
| review\_id | SERIAL | PRIMARY KEY. Auto-incrementing unique identifier for the review. |
| bank\_id | INT | FOREIGN KEY referencing banks(bank_id). |
| review\_text | TEXT | The cleaned review content. |
| rating | INT | The user-provided rating (1 to 5). |
| review\_date | DATE | The date the review was posted. |
| sentiment\_label | VARCHAR(50) | The NLP-derived sentiment ('Positive', 'Negative', 'Neutral'). |
| sentiment\_score | NUMERIC(5, 4) | The confidence score for the sentiment analysis. |

---

## KPIs Status

Based on the final preprocessing.py report:

| KPI | Target | Actual Result | Status | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Clean Volume | 1,200+ unique reviews collected | 1,517 clean reviews collected | ☑ Passed | Successfully exceeded the minimum target volume for a robust analysis. |
| Data Loss | <5% total loss | 15.72% total loss (283 rows) | ! Failed | The data loss was entirely due to the removal of duplicates, which is a necessary step for data quality. No critical data was missing. |
| DB Records | >1,000 reviews inserted | 1,515 reviews inserted | Passed | Confirmed via SQL count query. |

---

## Project Artifacts

* Clean CSV dataset: reviews_processed.csv available.
* Organized Git repo with clear commits: Commit history reflects logical chunks of work.