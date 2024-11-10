# CryptoAws
Bitcoin Cryptocurrency Dataset Analysis

This dataset includes key metrics such as Date, Open, Close, High, Low, Volume, and Market Cap.
<img width="1130" alt="Screenshot 2024-11-11 at 1 45 27 AM" src="https://github.com/user-attachments/assets/7bbf7bd0-a8fa-4538-a8ec-3093635b9273">


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
Calculate the average daily percentage change for Ethereum in 2016.
Identify the most volatile days for each cryptocurrency based on the highest percentage change between Open and Close.

1. dataput.py - This script uploads a file to an AWS S3 bucket using boto3 and AWS credentials loaded from an .env file
   <img width="938" alt="Screenshot 2024-11-11 at 1 27 35 AM" src="https://github.com/user-attachments/assets/609d2fdb-9b92-4fe1-a1b3-976001f46f82">

2. datafetchingfromaws.py - This script fetches cryptocurrency data from an S3 bucket, cleans and processes it using Spark, and performs analysis like identifying the highest volume days for Bitcoin, calculating Ethereum's average daily change, and finding the most volatile days for each cryptocurrency.
  <img width="1259" alt="Screenshot 2024-11-11 at 1 30 29 AM" src="https://github.com/user-attachments/assets/836cf8f6-23af-477f-acf0-a27e1b03297c">
  <img width="1259" alt="Screenshot 2024-11-11 at 1 30 39 AM" src="https://github.com/user-attachments/assets/07180844-94be-4e02-ba7f-c9655751fec8">


3. reducebykey.py - This script processes cryptocurrency data using PySpark, extracting year and month from the date, calculating total trading volume per month for each cryptocurrency using RDD's reduceByKey, and saves the result to a CSV file.
   <img width="1259" alt="Screenshot 2024-11-11 at 1 33 02 AM" src="https://github.com/user-attachments/assets/6212ad17-7736-49a1-8d80-dca2e1eff09b">
<img width="1259" alt="Screenshot 2024-11-11 at 1 33 17 AM" src="https://github.com/user-attachments/assets/998d92b2-75ae-4b29-b2e5-ef70283a9e71">

4. groupbyagreegation.py - This script processes cryptocurrency data using PySpark to:
Calculate total trading volume per cryptocurrency per week,the average daily price change per cryptocurrency and the total market cap per cryptocurrency.
<img width="1259" alt="Screenshot 2024-11-11 at 1 38 23 AM" src="https://github.com/user-attachments/assets/aeb442cc-1307-4a01-a9f7-df03a3b90676">
<img width="1259" alt="Screenshot 2024-11-11 at 1 39 02 AM" src="https://github.com/user-attachments/assets/040798bf-5792-4790-a5fe-405b1fb9d781">
<img width="1259" alt="Screenshot 2024-11-11 at 1 39 39 AM" src="https://github.com/user-attachments/assets/e5f278b6-3e90-4872-b44f-9b719a44f283">


5. finaldatainaws.py - This script uploads all .csv files from specified local folders to an S3 bucket while maintaining the folder structure.
<img width="1259" alt="Screenshot 2024-11-11 at 1 42 52 AM" src="https://github.com/user-attachments/assets/ddc3daa7-9dda-4577-b34f-a69476ac4610">

6. Cleaned_filtered_data – This folder contains the cleaned and filtered data based on Ethereum and Bitcoin.
7. Final_data_for_aws – This folder contains the results of various analyses, with answers to specific questions saved in separate CSV files.
<img width="392" alt="Screenshot 2024-11-11 at 1 43 57 AM" src="https://github.com/user-attachments/assets/2da79a8e-7f23-42ab-af07-6aae42e07fbb">



