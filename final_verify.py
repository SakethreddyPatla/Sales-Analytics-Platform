import duckdb
conn = duckdb.connect('data/sales_analytics.db', read_only=True)

print('=== DIM_CUSTOMERS ===')
print(conn.execute('SELECT COUNT(*) as total FROM main_marts.dim_customers').fetchdf())
print(conn.execute('SELECT customer_status, COUNT(*) as count FROM main_marts.dim_customers GROUP BY customer_status').fetchdf())

print('\n=== DIM_PRODUCTS ===')
print(conn.execute('SELECT COUNT(*) as total FROM main_marts.dim_products').fetchdf())
print(conn.execute('SELECT performance_tier, COUNT(*) as count FROM main_marts.dim_products GROUP BY performance_tier').fetchdf())

print('\n=== FCT_TRANSACTIONS ===')
print(conn.execute('SELECT COUNT(*) as total FROM main_marts.fct_transactions').fetchdf())
print(conn.execute('''
    SELECT 
        transaction_month,
        COUNT(*) as orders,
        ROUND(SUM(total_amount), 2) as revenue
    FROM main_marts.fct_transactions
    GROUP BY transaction_month
    ORDER BY transaction_month
''').fetchdf())

conn.close()
