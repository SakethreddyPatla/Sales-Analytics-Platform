# Orchestrates daily ETL process: EXTRACT -> LOAD -> TRANSFORM -> TEST

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount
import os


# Default arguments fro all tasks
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Defining the DAG

# Define the DAG
dag = DAG(
    'sales_analytics_pipeline',
    default_args=default_args,
    description='Daily sales data ETL pipeline',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'analytics', 'etl'],
)

# Task 1: Extract products and transactions from API
extract_api_data = DockerOperator(
    task_id='extract_api_data',
    image='sales_analytics_platform-extract:latest',
    command='python extract_api_data.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='sales_analytics_platform_sales_network',
    mounts=[
        Mount(
            source='C:/Users/saket/sales_analytics_platform/data',  
            target='/app/data',
            type='bind'
        ),
    ],
    auto_remove=True,
    mount_tmp_dir=False,
    dag=dag,
)

# Task 2: Generate customer data
generate_customers = DockerOperator(
    task_id='generate_customers',
    image='sales_analytics_platform-extract:latest',
    command='python generate_customer_data.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='sales_analytics_platform_sales_network',
    mounts=[
        Mount(
            source='C:/Users/saket/sales_analytics_platform/data',  
            target='/app/data',
            type='bind'
        ),
    ],
    auto_remove=True,
    mount_tmp_dir=False,
    dag=dag,
)

# Task 3: Load data to database
load_to_database = DockerOperator(
    task_id='load_to_database',
    image='sales_analytics_platform-load:latest',
    command='python load_to_db.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='sales_analytics_platform_sales_network',
    mounts=[
        Mount(
            source='C:/Users/saket/sales_analytics_platform/data',  
            target='/app/data',
            type='bind'
        ),
    ],
    auto_remove=True,
    mount_tmp_dir=False,
    dag=dag,
)

# Task 4: Verify data quality
verify_database = DockerOperator(
    task_id='verify_database',
    image='sales_analytics_platform-load:latest',
    command='python verify_database.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='sales_analytics_platform_sales_network',
    mounts=[
        Mount(
            source='C:/Users/saket/sales_analytics_platform/data',  
            target='/app/data',
            type='bind'
        ),
    ],
    auto_remove=True,
    mount_tmp_dir=False,
    dag=dag,
)

# Task 5: Send completion notification
send_notification = BashOperator(
    task_id='send_notification',
    bash_command='echo "Pipeline completed successfully at $(date)"',
    dag=dag,
)

# Define task dependencies (pipeline flow)
extract_api_data >> generate_customers >> load_to_database >> verify_database >> send_notification