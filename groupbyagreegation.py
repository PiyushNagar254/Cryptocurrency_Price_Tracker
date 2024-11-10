import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, abs, weekofyear, sum, avg
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoCurrencyAnalysis") \
    .getOrCreate()

# Path to the filtered data CSV file
filtered_data_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/filtered_data.csv'

# Load the CSV file into a Spark DataFrame
data_filtered_spark = spark.read.csv(filtered_data_path, header=True, inferSchema=True)

# Create TempView for the filtered data
data_filtered_spark.createOrReplaceTempView("filtered_crypto_data")

# Paths to save the outputs
total_trading_volume_week_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/total_trading_volume_week.csv'
avg_daily_change_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/avg_daily_change.csv'
total_market_cap_path = '/Users/piyushnagar/PycharmProjects/cryptoaws/total_market_cap.csv'

# 1. Total trading volume per cryptocurrency per week
total_trading_volume_week = spark.sql("""
    SELECT 
        Symbol, 
        weekofyear(Date) AS week, 
        SUM(Volume) AS total_trading_volume
    FROM filtered_crypto_data
    GROUP BY Symbol, weekofyear(Date)
    ORDER BY week DESC
""")

# Show the results
total_trading_volume_week.show()

# Save to CSV
total_trading_volume_week.write.csv(total_trading_volume_week_path, header=True, mode='overwrite')

# 2. Average daily price change per cryptocurrency
avg_daily_change = spark.sql("""
    SELECT 
        Symbol, 
        AVG((CAST(Close AS FLOAT) - CAST(Open AS FLOAT)) / CAST(Open AS FLOAT) * 100) AS avg_daily_percentage_change
    FROM filtered_crypto_data
    GROUP BY Symbol
    ORDER BY avg_daily_percentage_change DESC
""")

# Show the results
avg_daily_change.show()

# Save to CSV
avg_daily_change.write.csv(avg_daily_change_path, header=True, mode='overwrite')

# 3. Total market cap per cryptocurrency
total_market_cap = spark.sql("""
    SELECT 
        Symbol, 
        SUM(`Market Cap`) AS total_market_cap
    FROM filtered_crypto_data
    GROUP BY Symbol
    ORDER BY total_market_cap DESC
""")

# Show the results
total_market_cap.show()

# Save to CSV
total_market_cap.write.csv(total_market_cap_path, header=True, mode='overwrite')

# Stop the Spark session
spark.stop()
