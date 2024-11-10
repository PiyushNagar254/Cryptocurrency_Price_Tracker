from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, concat_ws

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoDataProcessing") \
    .getOrCreate()

# Load the dataset from the input CSV file
df = spark.read.csv("/Users/piyushnagar/PycharmProjects/cryptoaws/filtered_data.csv", header=True, inferSchema=True)

# Extract Year and Month from the Date column and create a YearMonth column
df = df.withColumn("YearMonth", concat_ws("-", year(df["Date"]), month(df["Date"])))

# Check the schema to verify the new column
df.printSchema()

# use the YearMonth column in RDD processing
rdd = df.rdd.map(lambda row: ((row['YearMonth'], row['Symbol']), row['Volume']))

# Using reduceByKey to calculate the total trading volume per month across all cryptocurrencies
result_rdd = rdd.reduceByKey(lambda x, y: x + y)

# Convert the RDD back to DataFrame
result_df = result_rdd.map(lambda x: (x[0][0], x[0][1], x[1])).toDF(["YearMonth", "Symbol", "Total_Trading_Volume"])

result_df.show()

# Save the result to the output CSV file
result_df.write.option("header", "true").csv("/Users/piyushnagar/PycharmProjects/cryptoaws/total_trading_volume_by_month.csv")
