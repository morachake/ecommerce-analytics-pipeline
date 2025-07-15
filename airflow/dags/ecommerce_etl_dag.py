from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import pandas as pd
import os
import logging

# Default arguments
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': True
}

# DAG definition
dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for ecommerce analytics',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    tags=['ecommerce', 'etl', 'analytics']
)

def load_csv_to_postgres(table_name, file_path, postgres_conn_id='warehouse_db'):
    """Load CSV data into PostgreSQL raw tables"""
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Add metadata columns
    df['loaded_at'] = datetime.now()
    df['file_name'] = os.path.basename(file_path)
    
    # Get PostgreSQL connection
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    # Truncate and load data (for demo purposes - in production use incremental)
    postgres_hook.run(f"TRUNCATE TABLE raw.{table_name}")
    
    # Insert data in chunks
    chunk_size = 10000
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        postgres_hook.insert_rows(
            table=f"raw.{table_name}",
            rows=chunk.values.tolist(),
            target_fields=chunk.columns.tolist(),
            commit_every=1000
        )
    
    logging.info(f"Loaded {len(df)} rows into raw.{table_name}")

def validate_data_quality(**context):
    """Run data quality checks on raw data"""
    
    postgres_hook = PostgresHook(postgres_conn_id='warehouse_db')
    
    quality_checks = [
        {
            'name': 'customers_no_nulls',
            'sql': "SELECT COUNT(*) FROM raw.customers WHERE customer_id IS NULL OR email IS NULL",
            'expected': 0
        },
        {
            'name': 'orders_valid_dates',
            'sql': "SELECT COUNT(*) FROM raw.orders WHERE order_date > CURRENT_DATE",
            'expected': 0
        },
        {
            'name': 'products_positive_prices',
            'sql': "SELECT COUNT(*) FROM raw.products WHERE price <= 0",
            'expected': 0
        },
        {
            'name': 'order_items_referential_integrity',
            'sql': """
                SELECT COUNT(*) FROM raw.order_items oi 
                LEFT JOIN raw.orders o ON oi.order_id = o.order_id 
                WHERE o.order_id IS NULL
            """,
            'expected': 0
        }
    ]
    
    failed_checks = []
    
    for check in quality_checks:
        result = postgres_hook.get_first(check['sql'])[0]
        if result != check['expected']:
            failed_checks.append(f"{check['name']}: expected {check['expected']}, got {result}")
    
    if failed_checks:
        raise ValueError(f"Data quality checks failed: {', '.join(failed_checks)}")
    
    logging.info("All data quality checks passed")

def generate_dbt_manifest(**context):
    """Generate dbt manifest for incremental processing"""
    execution_date = context['execution_date']
    
    # Create dbt vars file with execution date
    dbt_vars = {
        'execution_date': execution_date.strftime('%Y-%m-%d'),
        'start_date': (execution_date - timedelta(days=1)).strftime('%Y-%m-%d'),
        'end_date': execution_date.strftime('%Y-%m-%d')
    }
    
    vars_file_path = '/opt/airflow/dbt/vars.yml'
    with open(vars_file_path, 'w') as f:
        import yaml
        yaml.dump(dbt_vars, f)
    
    logging.info(f"Generated dbt vars for {execution_date}")

# File sensors to check for new data
file_sensors = []
tables = ['customers', 'products', 'orders', 'order_items', 'web_events']

for table in tables:
    sensor = FileSensor(
        task_id=f'sense_{table}_file',
        filepath=f'/opt/airflow/data/{table}.csv',
        poke_interval=60,
        timeout=300,
        dag=dag
    )
    file_sensors.append(sensor)

# Data loading tasks
load_tasks = []
for table in tables:
    load_task = PythonOperator(
        task_id=f'load_{table}',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'table_name': table,
            'file_path': f'/opt/airflow/data/{table}.csv'
        },
        dag=dag
    )
    load_tasks.append(load_task)

# Data quality validation
data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

# Generate dbt variables
dbt_vars_task = PythonOperator(
    task_id='generate_dbt_vars',
    python_callable=generate_dbt_manifest,
    dag=dag
)

# dbt staging models
dbt_staging = BashOperator(
    task_id='dbt_staging',
    bash_command='cd /opt/airflow/dbt && dbt run --models staging --vars-file vars.yml',
    dag=dag
)

# dbt warehouse models
dbt_warehouse = BashOperator(
    task_id='dbt_warehouse',
    bash_command='cd /opt/airflow/dbt && dbt run --models warehouse --vars-file vars.yml',
    dag=dag
)

# dbt marts models
dbt_marts = BashOperator(
    task_id='dbt_marts',
    bash_command='cd /opt/airflow/dbt && dbt run --models marts --vars-file vars.yml',
    dag=dag
)

# dbt tests
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --vars-file vars.yml',
    dag=dag
)

# Generate documentation
dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /opt/airflow/dbt && dbt docs generate',
    dag=dag
)

# Data quality monitoring
quality_report = PostgresOperator(
    task_id='generate_quality_report',
    postgres_conn_id='warehouse_db',
    sql="""
        INSERT INTO marts.data_quality_report (
            report_date, 
            total_customers, 
            total_orders, 
            total_revenue,
            avg_order_value,
            data_freshness_hours
        )
        SELECT 
            CURRENT_DATE,
            (SELECT COUNT(*) FROM warehouse.dim_customers WHERE is_current = true),
            (SELECT COUNT(*) FROM warehouse.fact_orders),
            (SELECT SUM(total_amount) FROM warehouse.fact_orders),
            (SELECT AVG(total_amount) FROM warehouse.fact_orders),
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(loaded_at)))/3600
        FROM raw.orders;
    """,
    dag=dag
)

# Set up dependencies
# File sensors trigger loads
for i, table in enumerate(tables):
    file_sensors[i] >> load_tasks[i]

# All loads must complete before quality check
load_tasks >> data_quality_task

# Generate dbt vars after quality check
data_quality_task >> dbt_vars_task

# dbt pipeline
dbt_vars_task >> dbt_staging >> dbt_warehouse >> dbt_marts >> dbt_test

# Documentation and reporting
dbt_test >> [dbt_docs, quality_report]