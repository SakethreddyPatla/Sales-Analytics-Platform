import duckdb
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

DB_PATH = os.getenv('DB_PATH', 'data/sales_analytics.db')

def get_latest_files():
    raw_dir = Path('/app/data/rawdata') if os.path.exists('/app/data') else Path('data/rawdata')
    product_files = sorted(raw_dir.glob('products_*.csv'))
    transactions_files = sorted(raw_dir.glob('transactions_*.csv'))
    customers_files = sorted(raw_dir.glob('customers_*.csv'))

    if not product_files or not transactions_files or not customers_files:
        raise FileNotFoundError("Missing data files. Run extraction")
    
    files = {
        'products' : str(product_files[-1]),
        'transactions' : str(transactions_files[-1]),
        'customers' : str(customers_files[-1])
    }

    print("Latest Files Found")
    for key, filepath in files.items():
        print(f'{key}: {filepath}')
    return files

def create_database():
    print(f'creating database at: {DB_PATH}')

    os.makedirs('data', exist_ok=True)

    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    print("Database initialized")
    return conn

def load_products(conn, filepath):
    print(f'Loading products from: {filepath}')
    df = pd.read_csv(filepath)

    conn.execute("DROP TABLE IF EXISTS raw.products")
    conn.execute("""
                 CREATE TABLE raw.products AS
                 SELECT * FROM df
                 """)
    
    count = conn.execute("SELECT COUNT(*) FROM raw.products").fetchone()[0]
    print(f'Loaded {count} products')

def load_transactions(conn, filepath):
    print(f'Loading transactions from:{filepath}')
    df=pd.read_csv(filepath)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    conn.execute("DROP TABLE IF EXISTS raw.transactions")
    conn.execute("""
                 CREATE TABLE raw.transactions AS
                 SELECT * FROM df
                 """)
    count = conn.execute("SELECT COUNT(*) FROM raw.transactions").fetchone()[0]
    print(f'loaded {count} transactions')

def load_customers(conn, filepath):
    print(f'Loading customers from: {filepath}')
    df = pd.read_csv(filepath)
    df['registration_date'] = pd.to_datetime(df['registration_date'])
    conn.execute("DROP TABLE IF EXISTS raw.customers")
    conn.execute("""
                 CREATE TABLE raw.customers AS
                 SELECT * FROM df
                 """)
    count = conn.execute("SELECT COUNT(*) FROM raw.customers").fetchone()[0]
    print(f'loaded {count} customers')

def verify_data(conn):
    print("Data Verification")
    tables = ['products', 'transactions', 'customers']

    for table in tables:
        result = conn.execute(f"SELECT COUNT(*) as count FROM raw.{table}").fetchdf()
        print(f"raw.{table}: {result['count'][0]} rows")

    print("Sample Transactions:")
    sample = conn.execute("""
                          SELECT
                            transaction_id,
                            transaction_date,
                            product_id,
                            product_title,
                            quantity,
                            total_amount
                          FROM raw.transactions
                          LIMIT 5
                          """).fetchdf()
    print(sample)

def create_metadata_table(conn):
    print("Creating metadata table")
    print(conn.execute("SELECT * FROM duckdb_tables() WHERE table_name='load_metadata'").fetchall())

    conn.execute("DROP TABLE IF EXISTS load_metadata")
    conn.execute("""
                 CREATE TABLE load_metadata (
                    load_id INTEGER,
                    load_timestamp TIMESTAMP,
                    table_name VARCHAR,
                    rows_loaded INTEGER,
                    source_file VARCHAR
                 )
""")
    timestamp = datetime.now()
    load_id = int(timestamp.timestamp()) # use timestamp as ID
    for table in ['products', 'transactions', 'customers']:
        count = conn.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        conn.execute("""
                     INSERT INTO load_metadata (load_id, load_timestamp, table_name, rows_loaded, source_file)
                     VALUES(?, ?, ?, ?, ?)
                     """, [load_id, timestamp, table, count, f'latest_{table}.csv'])
        print("Metadata table updated")

def main():
    print("Starting Data Load to Database")

    try:
        files = get_latest_files()
        conn = create_database()

        # Loading each table
        load_products(conn, files['products'])
        load_transactions(conn, files['transactions'])
        load_customers(conn, files['customers'])

        # Creating Metada
        create_metadata_table(conn)
        verify_data(conn)

        conn.close()

        print("Data Load Complete")
        print(f"Database Location: {DB_PATH}")

    except Exception as e:
        print(f"Error during load: {e}")
        raise
if __name__ == "__main__":
    main()