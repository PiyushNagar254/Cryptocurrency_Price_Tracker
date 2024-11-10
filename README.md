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

1. dataput.py - This script uploads a file to an AWS S3 bucket using boto3 and AWS credentials loaded from an .env file
2. datafetchingfromaws.py - This script fetches cryptocurrency data from an S3 bucket, cleans and processes it using Spark, and performs analysis like identifying the highest volume days for Bitcoin, calculating Ethereum's average daily change, and finding the most volatile days for each cryptocurrency.
3. reducebykey.py - This script processes cryptocurrency data using PySpark, extracting year and month from the date, calculating total trading volume per month for each cryptocurrency using RDD's reduceByKey, and saves the result to a CSV file.
4. groupbyagreegation.py - This script processes cryptocurrency data using PySpark to:
Calculate total trading volume per cryptocurrency per week,the average daily price change per cryptocurrency and the total market cap per cryptocurrency.
5. finaldatainaws.py - This script uploads all .csv files from specified local folders to an S3 bucket while maintaining the folder structure.
6. Cleaned_filtered_data – This folder contains the cleaned and filtered data based on Ethereum and Bitcoin.
7. Final_data_for_aws – This folder contains the results of various analyses, with answers to specific questions saved in separate CSV files.
