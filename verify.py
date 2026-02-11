
import duckdb
conn = duckdb.connect('data/sales_analytics.db', read_only=True)

print('=== CUSTOMERS TABLE ===')
customers = conn.execute('SELECT customer_id FROM raw.customers ORDER BY customer_id LIMIT 5').fetchdf()
print(customers)
print(f'Total: {conn.execute("SELECT COUNT(*) FROM raw.customers").fetchone()[0]}')

print('\n=== TRANSACTIONS TABLE ===')
txns = conn.execute('SELECT DISTINCT customer_id FROM raw.transactions ORDER BY customer_id LIMIT 5').fetchdf()
print(txns)

print('\n=== DO THEY MATCH? ===')
mismatch = conn.execute('''
    SELECT 
        COUNT(*) as total_transactions,
        COUNT(DISTINCT t.customer_id) as unique_customers_in_txns,
        SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) as orphaned_transactions
    FROM raw.transactions t
    LEFT JOIN raw.customers c ON t.customer_id = c.customer_id
''').fetchdf()
print(mismatch)

if mismatch['orphaned_transactions'].iloc[0] > 0:
    print('\n❌ PROBLEM: Transactions have customer IDs not in customers table!')
    print('\nSample mismatched IDs:')
    print(conn.execute('''
        SELECT DISTINCT t.customer_id
        FROM raw.transactions t
        LEFT JOIN raw.customers c ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        LIMIT 5
    ''').fetchdf())

conn.close()
