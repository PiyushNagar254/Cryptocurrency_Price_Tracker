import requests
import datetime
import pandas as pd
# Function to fetch cryptocurrency price data
def fetch_price_data(api_key):
    url = 'https://min-api.cryptocompare.com/data/pricemultifull'
    params = {
        'fsyms': 'BTC,ETH,LTC,XRP,DOT,ADA,SOL,BNB,MATIC',  # cryptocurrencies from symbols
        'tsyms': 'USD,EUR,GBP,AUD',  # currencies to symbols
    }

    # Headers including API key for authentication
    headers = {
        'Authorization': f'Apikey {api_key}',
        'Content-Type': 'application/json'
    }

    # Sending GET request with parameters and headers
    response = requests.get(url, headers=headers, params=params)
    return response.json()


# Function to process and display the fetched price data
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
    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'cryptocurrency_data_{current_time}.csv'
    df.to_csv(filename, index=False)
    print(f"Data saved to '{filename}'.")


def main():
    api_key = 'ff61c76130b422bfca7d6680ca64a95ec1c10ea4a6e784dda3bcb7d3570b14a0'
    data = fetch_price_data(api_key)
    display_price_data(data)


if __name__ == '__main__':
    main()


