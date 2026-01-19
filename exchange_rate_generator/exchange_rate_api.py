import requests
import json
import time
import random
import os
from datetime import datetime

# Create a folder named 'landing_zone' in the same directory as this script
OUTPUT_FOLDER = 'C:\\Users\\pa\\Documents\\data_project_1\\currency_rates'
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

foreign_currencies = ['EUR','GBP','JPY','CAD']

url = 'https://www.alphavantage.co/query?'

# --- API --- #
def get_exchangeRate ():
    
    print("Fetching exchange rates from Alpha Advantage...")
    rates = []
    for currency in range(len(foreign_currencies)):
        try:        
            params = {
                "function":'CURRENCY_EXCHANGE_RATE',
                "from_currency": foreign_currencies[currency],
                "to_currency": 'USD',
                "apikey":"KWEIAADILDL7CW63"
            }

            response = requests.get(url, params=params)
            data = response.json()
            rate = data.get("Realtime Currency Exchange Rate",[])
            rates.append(rate)
            time.sleep(2)

        except Exception as e:
            print(f"Error fetching API: {e}. Using fallback data.")
            rates += {
                "1. From_Currency Code": "ERR",
                "2. From_Currency Name": "Error Currency",
                "3. To_Currency Code": "ERR",
                "4. To_Currency Name": "Error Currency",
                "5. Exchange Rate": "0.0",
                "6. Last Refreshed": "1900-00-00 00:00:00",
                "7. Time Zone": "UTC",
                "8. Bid Price": "0.0",
                "9. Ask Price": "0.0"
                }
    return rates

def save_data():
    
    exchange_rates = get_exchangeRate()
    
    if not exchange_rates:
        print("No Exchange Rates found! Exiting.")
        return
    
    date_run = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    filename = f"{OUTPUT_FOLDER}\\exchange_rate_{date_run}.json"
    
    with open(filename, 'w') as f:
        json.dump(exchange_rates, f, indent=4)
    print(f"Saved file: exchange_rate_{date_run} ")

if __name__ == "__main__":
    save_data()