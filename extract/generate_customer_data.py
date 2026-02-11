"""
Generate synthetic customer data
Uses Faker library for realistic data
"""

from faker import Faker
import pandas as pd
from datetime import datetime
import random
import os

fake = Faker()

def generate_customers(num_customers=200):
    """Generate fake customer data"""
    print(f"Generating {num_customers} customers...")
    batch_id = datetime.now().strftime('%Y%m%d')
    customers = []
    
    for i in range(1, num_customers + 1):
        customer = {
            'customer_id': f'CUST{batch_id}_{i:04d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'country': 'USA',
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'customer_segment': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'created_at': datetime.now()
        }
        
        customers.append(customer)
    
    return pd.DataFrame(customers)

def save_customers(customers_df):
    """Save customer data to CSV"""
    output_dir = '/app/data/rawdata' if os.path.exists('/app/data') else 'data/rawdata'
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'{output_dir}/customers_{timestamp}.csv'
    
    customers_df.to_csv(filename, index=False)
    
    print(f"✅ Saved {len(customers_df)} customers to: {filename}")
    return filename

def main():
    """Main execution"""
    print("="*60)
    print("GENERATING CUSTOMER DATA")
    print("="*60)
    
    customers_df = generate_customers()
    save_customers(customers_df)
    
    print("="*60)
    print("CUSTOMER DATA GENERATION COMPLETE")
    print("="*60)

if __name__ == "__main__":
    main()