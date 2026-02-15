# Sales Analytics Platform

An end-to-end data engineering project simulating a production-grade sales analytics pipeline with automated orchestration, data transformation, quality testing, and dashboarding.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         AIRFLOW DAG                             │
│                    (Daily at 2 AM)                              │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌──────────────────┐  ┌─────────────────┐
│    EXTRACT      │  │      LOAD        │  │   TRANSFORM     │
│                 │  │                  │  │                 │
│ Fake Store API  │  │   DuckDB         │  │   dbt Models    │
│ Faker Library   │→ │   Raw Tables     │→ │   Staging       │
│ 200 Customers   │  │   - customers    │  │   Marts         │
│ 1000 Trans.     │  │   - products     │  │   31 Tests    │
│ 20 Products     │  │   - transactions │  │                 │
└─────────────────┘  └──────────────────┘  └─────────────────┘
                                                    │
                                                    ▼
                                          ┌─────────────────┐
                                          │   DASHBOARD     │
                                          │   Power BI      │
                                          │   3 Pages       │
                                          └─────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Apache Airflow 2.8.1 |
| Containerization | Docker & Docker Compose |
| Data Warehouse | DuckDB |
| Transformation | dbt (data build tool) 1.8.0 |
| Dashboard | Power BI |
| Language | Python 3.11 |
| Data Generation | Faker, Fake Store API |

---

## Pipeline Flow

```
1. generate_customers   → 200 synthetic customers (Faker library)
2. extract_api_data     → 20 products (Fake Store API) + 1000 transactions
3. load_to_database     → Load raw CSV → DuckDB warehouse
4. verify_database      → Data quality checks & validation
5. run_dbt_staging      → Staging views + 19 data tests 
6. run_dbt_marts        → dim/fct tables + 12 data tests 
7. send_notification    → Pipeline completion alert
```

---

## Project Structure

```
sales_analytics_platform/
├── airflow/
│   ├── dags/
│   │   └── sales_pipeline_dag.py    # Main Airflow DAG
│   ├── Dockerfile
│   └── requirements.txt
├── extract/
│   ├── extract_api_data.py          # API extraction + transactions
│   ├── generate_customer_data.py    # Synthetic customer generation
│   ├── Dockerfile
│   └── requirements.txt
├── load/
│   ├── load_to_db.py                # Load CSV → DuckDB
│   ├── verify_database.py           # Data quality verification
│   ├── Dockerfile
│   └── requirements.txt
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql    # Cleaned customers view
│   │   │   ├── stg_products.sql     # Cleaned products view
│   │   │   ├── stg_transactions.sql # Cleaned transactions view
│   │   │   └── schema.yml           # 19 staging tests
│   │   └── marts/
│   │       ├── dim_customers.sql    # Customer dimension + metrics
│   │       ├── dim_products.sql     # Product dimension + performance
│   │       ├── fct_transactions.sql # Enriched fact table
│   │       └── schema.yml           # 12 mart tests
│   ├── profiles.yml
│   └── dbt_project.yml
├── data/                            # Generated data (gitignored)
├── snapshots/                     # Pipeline screenshots
├── docker-compose.yml
└── README.md
```

---

## Data Model

```
RAW LAYER          STAGING LAYER          MARTS LAYER
──────────         ─────────────          ───────────
raw.customers  →   stg_customers     →    dim_customers
raw.products   →   stg_products      →    dim_products
raw.trans...   →   stg_transactions  →    fct_transactions
```

### dim_customers
- Customer details + purchase history
- Derived fields: `customer_status`, `purchase_frequency`
- Metrics: `lifetime_value`, `total_orders`, `avg_order_value`

### dim_products
- Product details + sales performance
- Derived fields: `performance_tier`, `price_tier`
- Metrics: `total_revenue`, `total_units_sold`

### fct_transactions
- Enriched transactions with customer & product context
- Date dimensions: day, week, month, year
- Metrics: `estimated_profit`, `revenue_per_unit`

---

## Data Quality Tests

```
Staging Layer:   19/19 tests passing 
Marts Layer:     12/12 tests passing 
─────────────────────────────────────
Total:           31/31 tests passing 
```

Tests include: `not_null`, `unique`, `accepted_values`, `relationships`, `accepted_range`

---

## Power BI Dashboard

3-page interactive dashboard:

- **Page 1 - Sales Overview:** Total Revenue, Orders, Avg Order Value, Revenue by Month, Revenue by Category
- **Page 2 - Customer Analysis:** Segment Distribution, Customer Status, Top Customers by Revenue
- **Page 3 - Product Performance:** Revenue by Category, Performance Tiers, Product Rankings

---

## How to Run

### Prerequisites
- Docker Desktop
- Python 3.11+
- Power BI Desktop (for dashboard)

### Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/sales-analytics-platform.git
cd sales-analytics-platform

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux

# 3. Start Airflow & all services
docker-compose up -d

# 4. Wait 30 seconds, then open Airflow UI
# http://localhost:8080
# Username: admin | Password: admin

# 5. Trigger the DAG
# Toggle ON → Click ▶ Trigger DAG
```

### Manual Run (without Airflow)

```bash
# Extract data
docker-compose run --rm extract python generate_customer_data.py
docker-compose run --rm extract python extract_api_data.py

# Load to database
docker-compose run --rm load python load_to_db.py

# Run dbt transformations
cd dbt_project
dbt run
dbt test
```

---

## 📸 Screenshots

### Airflow Pipeline - All Tasks Successful
![Airflow DAG](screenshots/airflow_dag_success.png)

### Power BI Dashboard
![Dashboard](screenshots/powerbi_dashboard.png)

---

## Key Features

- Fully automated daily pipeline via Airflow
- Containerized with Docker for reproducibility
- 31 data quality tests (all passing)
- 3-layer data warehouse architecture (raw → staging → marts)
- Incremental-ready pipeline design
- Cross-platform compatible (Windows/Linux)
- Interactive Power BI dashboard

---

## Production Considerations

In a real production environment:
- Replace `generate_customer_data.py` → Extract from CRM (Salesforce, HubSpot)
- Replace `extract_api_data.py` → Extract from e-commerce platform (Shopify, Stripe)
- Replace DuckDB → Cloud warehouse (Snowflake, BigQuery, Redshift)
- Add alerting → Email/Slack notifications on failure
- Add incremental models → Process only new/changed records daily

---

## Author

**Saketh Reddy Patla**
- GitHub: [SakethreddyPatla](https://github.com/SakethreddyPatla)
