import requests
import json
import time
import random
import os
from datetime import datetime

# --- CONFIGURATION ---
# Create a folder named 'landing_zone' in the same directory as this script
OUTPUT_FOLDER = 'C:\\Users\\pa\\Documents\\data_project_1\\orders'
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# --- OPEN FOOD FACTS API ---
# We fetch valid products so our data looks real
def get_products():
    print("Fetching product list from Open Food Facts...")
    try:
        # Searching for popular categories to get a good mix of products
        # We request specific fields to keep the payload light
        url = "https://world.openfoodfacts.org/cgi/search.pl"
        params = {
            "search_terms": "snack",
            "search_simple": 1,
            "action": "process",
            "json": 1,
            "page_size": 50,
            "fields": "code,product_name,brands,categories_tags"
        }
        response = requests.get(url, params=params)
        data = response.json() #.json() is part of response module, makes json string into dictionary
        return data.get('products', []) #different builtin .get() method, part of all libraries, works on dictionary types
    except Exception as e:
        print(f"Error fetching API: {e}. Using fallback data.")
        return [{"code": "000000", "product_name": "Fallback Snack", "brands": "Generic", "categories_tags": ["en:snacks"]}]

# --- GENERATOR LOGIC ---
def generate_stream():
    products = get_products()
    if not products:
        print("No products found! Exiting.")
        return

    currencies = ['EUR', 'GBP', 'JPY', 'CAD']
    
    print("Starting Data Stream Simulation... (Press Ctrl+C to stop)")
    print(f"Loaded {len(products)} products from Open Food Facts.")
    
    transaction_id = 1000
    counter = 0

    while counter < 31:
        
        # 1. Pick a random product
        product = random.choice(products)
        
        # 2. Pick a random currency
        currency = random.choice(currencies)
        
        # 3. Simulate a price (Since OFF doesn't always have price, we generate a realistic one)
        base_price_usd = random.uniform(2.50, 15.00)
        
        # ED TODO: Later, we will add logic here to make JPY numbers look realistic (multiply by 100)
        # For now, we just add random fluctuation to simulate dynamic pricing
        local_price_multiplier = random.uniform(0.9, 1.1) 
        local_price = round(base_price_usd * local_price_multiplier, 2)

        # 3. Create the Transaction Record
        transaction = {
            "transaction_id": transaction_id,
            "timestamp": datetime.now().isoformat(),
            "product_code": product.get('code', 'N/A'),
            "product_name": product.get('product_name', 'Unknown Product'),
            "brand": product.get('brands', 'Unknown Brand'),
            "currency": currency,
            "local_price": local_price,
            "quantity": random.randint(1, 5),
            "store_id": random.randint(1, 50),
            # ED CHALLENGE: Add a 'customer_loyalty_id' field here
            "customer_loyalty_id": random.randint(77777,10000000)
        }
        
        # 4. Save to JSON file (Simulating a "Batch" arriving)
        filename = f"{OUTPUT_FOLDER}/order_{transaction_id}.json"
        with open(filename, 'w') as f:
            json.dump(transaction, f)
            
        print(f"Generated Order #{transaction_id}: {transaction['product_name']} ({currency})")
        
        counter += 1
        transaction_id += 1
        time.sleep(2) # Wait 2 seconds before next order
        
        
if __name__ == "__main__":
    generate_stream()
