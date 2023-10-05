from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'rgurung'
}

with DAG(
    dag_id = 'postgres_pipeline',
    description = 'Running a pipeline using the Postgres operator',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'postgres'],
    template_searchpath = '/Users/reginagurung/airflow/sql_statements'
) as dag:
    
    create_table_customers = PostgresOperator(
        task_id = 'create_table_customers',
        postgres_conn_id = 'postgres_conn',
        sql = 'create_table_customers.sql'
    )

    create_table_customer_purchases = PostgresOperator(
        task_id = 'create_table_customer_purchases',
        postgres_conn_id = 'postgres_conn',
        sql = 'create_table_customer_purchases.sql'
    )

create_table_customers >> create_table_customer_purchases
