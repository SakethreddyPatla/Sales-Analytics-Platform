import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import random
import os

def fetch_products():
    print("Fetching products from API")
    response = requests.get('https://fakestoreapi.com/products')
    products = response.json()
    df = pd.DataFrame(products)
    df['extracted_at'] = datetime.now()
    return df

def generate_transactions(products_df, num_transactions = 1000):
    print(f"Generating {num_transactions} transactions")
    transactions=[]
    start_date = datetime.now() - timedelta(days=90)

    for i in range(num_transactions):
        # Random date within last 90 days
        random_days = random.randint(0, 90)
        transaction_date = start_date + timedelta(days=random_days)
        # Random product
        product = products_df.sample(1).iloc[0]

        # Random quantity
        quantity = random.randint(1, 5)

        # Calculate total
        unit_price = float(product['price'])
        total_amount = unit_price * quantity

        transaction = {
            'transaction_id': f'TXN{str(i+1).zfill(6)}',
            'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product['id'],
            'product_title': product['title'],
            'category': product['category'],
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': round(total_amount, 2),
            'customer_id': f'CUST{random.randint(1, 200):04d}',
            'extracted_at': datetime.now()
        }

        transactions.append(transaction)
    
    return pd.DataFrame(transactions)

def save_data(products_df, transactions_df):
    output_dir = 'data/raw'
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    products_file = f'{output_dir}/products_{timestamp}.csv'
    transactions_file = f'{output_dir}/transactions_{timestamp}.csv'

    products_df.to_csv(products_file, index=False)
    transactions_df.to_csv(transactions_file, index=False)

    print(f'Saved Products to: {products_file}')
    print(f'Saved transactions to: {transactions_file}')

    return products_file, transactions_file

def main():
    print("Starting Data Extraction")

    products_df = fetch_products()
    print(f"Fetched{len(products_df)} products")

    transactions_df = generate_transactions(products_df)
    print(f'Generated {len(transactions_df)} transactions')

    products_file, transaction_file = save_data(products_df, transactions_df)

    print("Data Extraction Complete")

if __name__ == "__main__":
    main()
