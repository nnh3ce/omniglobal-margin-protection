import requests
import json
import time
import random
import os
from datetime import datetime



# --- API --- #
def get_exchangeRate ():
    # Create a folder named 'landing_zone' in the same directory as this script
    OUTPUT_FOLDER = '/Volumes/omniglobal__margin_protection/omni_bronze/ingest_files/exchange_rates/'
    #'C:\\Users\\pa\\Documents\\data_project_1\\exchange_rates' # for testing
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    foreign_currencies = ['EUR','GBP','JPY','CAD']

    url = 'https://www.alphavantage.co/query?'  
    print("Fetching exchange rates from Alpha Advantage...")
    #rates = []
    #cleaned_rates = []
    
    for currency in range(0,4):
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
            
        #clean up: remove spaces in key values for databricks column requirement
        cleaned_rate = {k.replace(" ", "_"): v for k, v in rate.items()}

    
        if not cleaned_rate:
            print("No Exchange Rates found! Exiting.")
            return
    
        date_run = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        filename = f"{OUTPUT_FOLDER}\\exchange_rate_{date_run}.json"
    
        with open(filename, 'w') as f:
            json.dump(cleaned_rate, f, indent=4)
        print(f"Saved file: exchange_rate_{date_run} ")

if __name__ == "__main__":
    get_exchangeRate()