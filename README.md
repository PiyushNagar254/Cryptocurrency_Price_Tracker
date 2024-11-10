# Data_Extraction
Cryptocurrency Price Tracker:
A Python script that fetches real-time cryptocurrency prices, market cap, and 24-hour changes using the CryptoCompare API. The data is saved to a CSV file with the current date in the filename.It Fetches prices for popular cryptocurrencies in USD, EUR, GBP, and AUD and saves data to a CSV file named cryptocurrency_data_yyyymmdd_h m s.csv.
Required Libraries: requests, pandas, and datetime


# CryptoAws
Bitcoin Cryptocurrency Dataset Analysis

This dataset includes key metrics such as Date, Open, Close, High, Low, Volume, and Market Cap.

Analysis Steps:
Data Cleaning:
Remove rows with missing values in key columns (Open, Close, High, Low).
Remove duplicate records (if multiple entries exist for the same date).
Filtering:
Filter data for days with trading volume above a specific threshold (> $1M).
Focus on specific cryptocurrencies like Bitcoin and Ethereum.
Feature Engineering:
Calculate the daily percentage change in price using:
((Close - Open) / Open) * 100.
Classify the daily change as Up, Down, or Neutral based on the percentage change.
Aggregation:
Group data by cryptocurrency and aggregate:
Total trading volume (weekly).
Average daily price change.
Total market cap.
Monthly Aggregation:
Use reduceByKey to calculate the total trading volume across all cryptocurrencies on a monthly basis.
Spark SQL:
Create a TempView for the data and use Spark SQL to:
Identify the days with the highest trading volume for Bitcoin.
Calculate the average daily percentage change for Ethereum in 2023.
Identify the most volatile days for each cryptocurrency based on the highest percentage change between Open and Close.



