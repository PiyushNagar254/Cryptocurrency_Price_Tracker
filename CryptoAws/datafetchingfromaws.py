import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, abs, row_number
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoCurrencyAnalysis") \
    .getOrCreate()

# Retrieve AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize the S3 client with credentials from the environment
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)


# Function to fetch data from S3 without saving locally
def fetch_from_s3(bucket_name, object_name):
    try:
        file_stream = io.BytesIO()
        s3.download_fileobj(bucket_name, object_name, file_stream)
        file_stream.seek(0)
        df = pd.read_csv(file_stream)
        print("Data fetched successfully")
        return df
    except NoCredentialsError:
        print("Credentials not available")
    except ClientError as e:
        print(f"Failed to fetch the file: {e}")

# Define bucket name and object name
bucket_name = 'piyush-crypto-currency'
object_name = 'all_currencies.csv'

# Fetch data into a pandas DataFrame
data = fetch_from_s3(bucket_name, object_name)

# Data cleaning and preparation
data['Volume'] = data['Volume'].replace({',': ''}, regex=True)
data['Market Cap'] = data['Market Cap'].replace({',': ''}, regex=True)
data['Volume'] = pd.to_numeric(data['Volume'], errors='coerce')
data['Market Cap'] = pd.to_numeric(data['Market Cap'], errors='coerce')
data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
data.dropna(subset=['Open', 'Close', 'High', 'Low', 'Date'], inplace=True)
data.drop_duplicates(subset=['Date', 'Symbol'], keep='first', inplace=True)

# Add a daily percentage change column and categorize it
data['daily_percentage_change'] = (data['Close'] - data['Open']) / data['Open'] * 100
data['change_type'] = data['daily_percentage_change'].apply(
    lambda x: 'Up' if x > 0 else ('Down' if x < 0 else 'Neutral')
)

# Step 1: Filter for volume > 1 million to create dataframe1
data_filtered = data[data['Volume'] > 1e6]
data_filtered_spark = spark.createDataFrame(data_filtered)

# Save filtered data to a CSV file (after volume filter)
filtered_data_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/filtered_data.csv'
data_filtered.to_csv(filtered_data_path, index=False)

# Create TempView for dataframe1 (filtered by volume > 1 million)
data_filtered_spark.createOrReplaceTempView("filtered_crypto_data")

# Step 2: Further filter for Ethereum and Bitcoin to create dataframe2
data_filtered_btc_eth = data_filtered[data_filtered['Symbol'].isin(['BTC', 'ETH'])]
data_filtered_btc_eth_spark = spark.createDataFrame(data_filtered_btc_eth)

# Save filtered data for BTC and ETH to a CSV file
btc_eth_filtered_data_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/btc_eth_filtered_data.csv'
data_filtered_btc_eth.to_csv(btc_eth_filtered_data_path, index=False)

# Create TempView for dataframe2 (filtered for BTC and ETH)
data_filtered_btc_eth_spark.createOrReplaceTempView("btc_eth_data")

# Define output paths for CSV files
highest_volume_bitcoin_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/highest_volume_bitcoin.csv'
avg_daily_change_ethereum_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/avg_daily_change_ethereum.csv'
most_volatile_days_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/most_volatile_days.csv'

# 1. Find the days with the highest trading volume for Bitcoin using dataframe2 (BTC & ETH only)
highest_volume_bitcoin = spark.sql("""
    SELECT Date, Symbol, Volume
    FROM btc_eth_data
    WHERE Symbol = 'BTC'
    ORDER BY Volume DESC
    LIMIT 1
""")
highest_volume_bitcoin.show()
highest_volume_bitcoin.write.csv(highest_volume_bitcoin_path, header=True, mode='overwrite')

# 2. Calculate the average daily percentage change in price for Ethereum in 2023 using dataframe2 (BTC & ETH only)
avg_daily_change_ethereum = spark.sql("""
    SELECT AVG((CAST(Close AS FLOAT) - CAST(Open AS FLOAT)) / CAST(Open AS FLOAT) * 100) AS avg_daily_percentage_change
    FROM btc_eth_data
    WHERE Symbol = 'ETH' AND year(Date) = 2016
""")
avg_daily_change_ethereum.show()
avg_daily_change_ethereum.write.csv(avg_daily_change_ethereum_path, header=True, mode='overwrite')

# 3. Identify the most volatile day for each cryptocurrency based on the highest percentage change using dataframe1 (all cryptocurrencies)
window_spec = Window.partitionBy('Symbol').orderBy(abs((col('Close') - col('Open')) / col('Open') * 100).desc())
most_volatile_days = data_filtered_spark.withColumn(
    'volatility_percentage',
    abs((col('Close') - col('Open')) / col('Open') * 100)
).withColumn(
    'row_num', row_number().over(window_spec)
).filter(col('row_num') == 1).drop('row_num')

most_volatile_days.show()
most_volatile_days.write.csv(most_volatile_days_path, header=True, mode='overwrite')

# Stop the Spark session
spark.stop()


