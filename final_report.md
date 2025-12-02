Rufi, [12/2/2025 10:14 PM]
# 🏦 Customer Experience Analytics: Mobile Banking Apps in Ethiopia

## Executive Summary

This report presents key findings from analyzing over 1,500 user reviews for the mobile banking applications of Commercial Bank of Ethiopia (CBE), Bank of Abyssinia (BOA), and Dashen Bank. The analysis identifies critical user satisfaction drivers and significant pain points, delivering targeted recommendations to improve customer retention and app stability.

### Key Findings:

* Overall Sentiment: The aggregated sentiment analysis shows a 51.4% skew towards Positive reviews overall, but the 15.2% Negative segment highlights critical systemic failures.
* Top Pain Points: The most common complaints revolve around work, errors, and login issues, indicating serious problems with core stability and access.
* Highest Performer: Dashen Bank currently holds the highest average user rating at 4.09.

---

## 1. Methodology and Data

The data was collected via scraping Google Play Store reviews, processed for duplicates and missing values, and augmented with automated sentiment analysis. The final dataset of 1,515 unique reviews was stored in a normalized PostgreSQL database (bank\_reviews) for robust retrieval and analysis.

---

## 2. Quantitative Visualization

The following charts summarize the rating and sentiment distribution across the three competing banks.

### 2.1. Rating Distribution (1-5 Stars)

This chart shows how users distribute their scores. A high volume of 1-star ratings is a strong indicator of critical bugs and poor performance.



### 2.2. Sentiment Distribution

This plot shows the volume of Positive, Negative, and Neutral reviews derived from the NLP analysis.



---

## 3. Bank-Specific Analysis & Insights

This section details the specific drivers (what users like) and pain points (what users complain about) for each institution, based on ratings and keyword frequency from their reviews.

### 3.1. Dashen Bank (Highest Performer)

* Average Rating: 4.09
* Drivers (What Works): *dashen, banking, super* (Indicates satisfaction with the overall brand experience and the "SuperApp" concept.)
* Pain Points (What Fails): *slow, worst, banking* (Despite the high rating, key issues are general performance and critical failure.)

Recommendations for Dashen Bank:

1.  Performance: Investigate and resolve primary stability issues reported in low-rating reviews, focusing on general speed and resource consumption.
2.  Feature Enhancement: Introduce user-requested features like multi-currency account views or integrated customer support chat.

### 3.2. Commercial Bank of Ethiopia (CBE)

* Average Rating: 3.97
* Drivers (What Works): *branch, payment, service* (Suggests the integration with physical branches and core payment services is appreciated.)
* Pain Points (What Fails): *errors, network, failed* (Indicates instability during critical operations like transactions or network handoffs.)

Recommendations for CBE:

1.  Transaction Stability: Implement robust rollback mechanisms and provide real-time status updates for all fund transfers to prevent data loss confusion caused by "failed" transactions.
2.  Feature Enhancement: Introduce user-requested features like multi-currency account views or integrated customer support chat.

### 3.3. Bank of Abyssinia (BOA)

* Average Rating: 3.81
* Drivers (What Works): *interface, easy, simple* (Users appreciate the usability and clean design of the application.)
* Pain Points (What Fails): *login, support, crash* (Highlights critical barriers to entry and use, specifically security/access and fundamental stability.)

Recommendations for BOA:

1.  User Access: Streamline the login process with options for persistent biometric/PIN access to reduce server strain from repeated attempts and resolve frequent login issues.
2.  Feature Enhancement: Introduce user-requested features like multi-currency account views or integrated customer support chat.

### 3.4. Deep Dive: Top Pain Points

This word cloud visualizes the most frequently mentioned keywords in the negative reviews for the lowest-rated bank, clearly identifying the most critical areas for immediate developer focus.



---

## 4. Ethical Considerations and Bias

It is important to note the potential for bias in this analysis (Ethics Task):

* Negative Skew: App reviews often exhibit a negative skew, as highly satisfied customers are less likely to leave a review than frustrated customers experiencing a crash or login failure. The true average sentiment may be slightly higher than calculated.
* Language Limitations: Sentiment models, while robust, may struggle with local dialect or context-specific slang, potentially misclassifying some reviews.

---

## Conclusion

The analysis provides a clear roadmap for mobile app improvement, prioritizing stability and core functionality. Addressing the core pain points identified (such as login and transaction stability) will yield the greatest immediate impact on customer satisfaction and reduce negative sentiment volume.