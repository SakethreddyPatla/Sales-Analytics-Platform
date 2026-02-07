import duckdb

DB_PATH = 'data/sales_analytics.db'

def main():
    conn = duckdb.connect(DB_PATH, read_only=True)
    print("Database Summary")

    print("Available Tables:")
    tables = conn.execute("SHOW TABLES").fetchdf()
    print(tables)

    print("Transaction Summary:")
    summary = conn.execute("""
                           SELECT
                            COUNT(*) AS total_transactions,
                            COUNT(DISTINCT customer_id) AS unique_customers,
                            COUNT(DISTINCT product_id) AS unique_products,
                            ROUND(SUM(total_amount), 2) AS total_revenue,
                            ROUND(AVG(total_amount), 2) AS avg_transaction_value,
                            MIN(transaction_date) AS earliest_transaction_date,
                            MAX(transaction_date) AS latest_transaction_date
                           FROM raw.transactions
                           """).fetchdf()
    print(summary)

    # Top 5 products by Revenue
    print("Top 5 products by Revenue:")
    top_products = conn.execute("""
                                SELECT
                                    product_title
                                    category,
                                    ROUND(SUM(total_amount), 2) AS revenue,
                                    COUNT(*) AS unit_sold
                                FROM raw.transactions
                                GROUP BY product_title, category
                                ORDER BY revenue DESC
                                LIMIT 5
                                """).fetchdf()
    print(top_products)

    # Customer Segments
    print("Customer Segments: ")
    segments = conn.execute("""
                            SELECT
                                customer_segment,
                                COUNT(*) as customer_count,
                                ROUND(COUNT(*) * 100 / SUM(COUNT(*)) OVER(), 1) as pct
                            FROM raw.customers
                            GROUP BY customer_segment
                            ORDER BY pct DESC
                            """).fetchdf()
    print(segments)

    # Sales by Category
    print("Sales by Category: ")
    categories = conn.execute("""
                              SELECT
                                category,
                                COUNT(*) as transactions,
                                ROUND(SUM(total_amount), 2) AS revenue
                              FROM raw.transactions
                              GROUP BY category
                              ORDER BY revenue DESC
                              """).fetchdf()
    print(categories)

    conn.close()
if __name__ == "__main__":
    main()