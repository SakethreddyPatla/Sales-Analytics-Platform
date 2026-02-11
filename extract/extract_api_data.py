import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import random
import os
import glob

def fetch_products():
    print("Fetching products from API")
    response = requests.get('https://fakestoreapi.com/products')
    products = response.json()
    df = pd.DataFrame(products)
    df['extracted_at'] = datetime.now()
    return df

def generate_transactions(products_df, num_transactions = 1000, customer_ids=None):
    print(f"Generating {num_transactions} transactions")
    batch_id = datetime.now().strftime('%Y%m%d')
    print(f"Batch ID: {batch_id}")
    if customer_ids is not None:
        print(f"Using {len(customer_ids)} provided customer IDs")
        customer_pool = customer_ids
    else:
        print("No customer IDs provided, using placeholders")
        customer_pool = [f'CUST{batch_id}_{i:04d}' for i in range(1, 201)]
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
            'transaction_id': f'TXN{batch_id}_{str(i+1).zfill(6)}',
            'transaction_date': transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
            'product_id': product['id'],
            'product_title': product['title'],
            'category': product['category'],
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': round(total_amount, 2),
            'customer_id': f'CUST{batch_id}_{random.randint(1, 200):04d}',
            'extracted_at': datetime.now()
        }

        transactions.append(transaction)
    
    return pd.DataFrame(transactions)

def save_data(products_df, transactions_df):
    output_dir = '/app/data/rawdata' if os.path.exists('/app/data') else 'data/rawdata'
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    products_file = f'{output_dir}/products_{timestamp}.csv'
    transactions_file = f'{output_dir}/transactions_{timestamp}.csv'

    products_df.to_csv(products_file, index=False)
    transactions_df.to_csv(transactions_file, index=False)

    print(f'Saved Products to: {products_file}')
    print(f'Saved transactions to: {transactions_file}')

    return products_file, transactions_file


def load_customer_ids(data_dir='data/rawdata'):
    """Load customer IDs from the most recent customer file"""
    
    # Check for customer files
    customer_files = sorted(glob.glob(f'{data_dir}/customers_*.csv'))
    
    if not customer_files:
        print("No customer files found")
        return None
    
    # Load most recent customer file
    latest_customer_file = customer_files[-1]
    print(f"Loading customer IDs from: {os.path.basename(latest_customer_file)}")
    
    customers_df = pd.read_csv(latest_customer_file)
    customer_ids = customers_df['customer_id'].tolist()
    print(f"Loaded {len(customer_ids)} customer IDs")
    return customer_ids
def main():
    print("Starting Data Extraction")

    products_df = fetch_products()
    print(f"Fetched{len(products_df)} products")
    data_dir = '/app/data/rawdata' if os.path.exists('/app/data') else 'data/rawdata'
    print(f"\nLooking for customers in: {data_dir}")
    customer_ids = load_customer_ids(data_dir)
    
    if customer_ids is None:
        print("\nERROR: No customers found!")
        print("Please run: docker-compose run --rm extract python generate_customer_data.py")
        print("Or locally: python extract/generate_customer_data.py")
        return
    transactions_df = generate_transactions(products_df, customer_ids=customer_ids)
    print(f'Generated {len(transactions_df)} transactions')

    products_file, transaction_file = save_data(products_df, transactions_df)

    print("Data Extraction Complete")

if __name__ == "__main__":
    main()
