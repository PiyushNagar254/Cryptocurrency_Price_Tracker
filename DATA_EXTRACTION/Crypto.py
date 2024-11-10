import requests
import datetime
import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
# Function to fetch cryptocurrency price data
def fetch_price_data(api_key):
    url = 'https://min-api.cryptocompare.com/data/pricemultifull'
    params = {
        'fsyms': 'BTC,ETH,LTC,XRP,DOT,ADA,SOL,BNB,MATIC',  # cryptocurrencies from symbols
        'tsyms': 'USD,EUR,GBP,AUD',  # currencies to symbols
    }
    headers = {
        'Authorization': f'Apikey {api_key}',
        'Content-Type': 'application/json'
    }

    # Sending GET request with parameters and headers
    response = requests.get(url, headers=headers, params=params)
    return response.json()


# Function to process and save the fetched price data into MySQL
def save_to_mysql(data, db_conn, cursor):
    records = []
    for coin, info in data['RAW'].items():
        for currency, price_info in info.items():
            last_update = datetime.datetime.fromtimestamp(price_info['LASTUPDATE'])
            record = (
                coin,
                currency,
                price_info['PRICE'],
                price_info['MKTCAP'],
                price_info['VOLUME24HOUR'],
                price_info['CHANGE24HOUR'],
                price_info['CHANGEPCT24HOUR'],
                last_update
            )
            records.append(record)

    # Insert data into MySQL
    insert_query = """
    INSERT INTO cryptocurrency (Cryptocurrency, Currency, Price, MarketCap, Volume24h, Change24h, ChangePct24h, LastUpdate)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    Price = VALUES(Price), MarketCap = VALUES(MarketCap), Volume24h = VALUES(Volume24h), 
    Change24h = VALUES(Change24h), ChangePct24h = VALUES(ChangePct24h), LastUpdate = VALUES(LastUpdate);
    """
    cursor.executemany(insert_query, records)
    db_conn.commit()
    print(f"{cursor.rowcount} rows inserted/updated in the database.")


# Function to process and display the data
def display_price_data(data):
    records = []
    for coin, info in data['RAW'].items():
        for currency, price_info in info.items():
            last_update = datetime.datetime.fromtimestamp(price_info['LASTUPDATE'])
            record = {
                'Cryptocurrency': coin,
                'Currency': currency,
                'Price': price_info['PRICE'],
                'Market Cap': price_info['MKTCAP'],
                'Volume (24h)': price_info['VOLUME24HOUR'],
                'Change (24h)': price_info['CHANGE24HOUR'],
                'Change (%)': price_info['CHANGEPCT24HOUR'],
                'Last Update': last_update
            }
            records.append(record)

    df = pd.DataFrame(records)
    print(df)

    # Save the data frame
    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'cryptocurrency_data_{current_time}.csv'
    df.to_csv(filename, index=False)
    print(f"Data saved to '{filename}'.")


def main():
    api_key = os.getenv('CRYPTO_API_KEY')

    # Connect to MySQL
    db_conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DB')
    )
    cursor = db_conn.cursor()
    data = fetch_price_data(api_key)
    save_to_mysql(data, db_conn, cursor)
    display_price_data(data)
    cursor.close()
    db_conn.close()
if __name__ == '__main__':
    main()
