from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.sql_sensor import SqlSensor

default_args = {
    'owner': 'rgurung'
}

with DAG(
    dag_id = 'pipeline_with_SqlSensor',
    description = 'Executing pipeline with SqlSensor Postgres',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['postgres', 'sql_sensor', 'sensor']
) as dag:
    
    create_laptops_table = PostgresOperator(
        task_id = 'create_laptops_table',
        postgres_conn_id = 'postgres_conn',
        
        sql = """
            CREATE TABLE IF NOT EXISTS laptops (
                    id SERIAL PRIMARY KEY,
                    company VARCHAR(255),
                    product VARCHAR(255),
                    type_name VARCHAR(255),
                    price_euros NUMERIC(10,2)
            );
        """
    )

    create_premium_laptops_table = PostgresOperator(
        task_id = 'create_premium_laptops_table',
        postgres_conn_id = 'postgres_conn',
        sql = """
            CREATE TABLE IF NOT EXISTS premium_laptops (
                    id SERIAL PRIMARY KEY,
                    company VARCHAR(255),
                    product VARCHAR(255),
                    type_name VARCHAR(255),
                    price_euros NUMERIC(10,2)
            );
        """
    )

    wait_for_premium_laptops = SqlSensor(
        task_id = 'wait_for_premium_laptops',
        conn_id = 'postgres_conn',
        sql = "SELECT EXISTS(SELECT 1 FROM laptops WHERE price_euros > 500)",
        poke_interval = 10,
        timeout = 10 * 60
    )

    insert_data_into_premium_laptops_table = PostgresOperator(
        task_id = 'insert_data_into_premium_laptops_table',
        postgres_conn_id = 'postgres_conn',
        sql = """INSERT INTO premium_laptops
                 SELECT * FROM laptops WHERE price_euros > 500
        """
    )

    delete_laptop_data = PostgresOperator(
        task_id = 'delete_laptop_data',
        postgres_conn_id = 'postgres_conn',
        sql = "DELETE FROM laptops"
    )

    chain(
        [create_laptops_table, create_premium_laptops_table],
        wait_for_premium_laptops,
        insert_data_into_premium_laptops_table,
        delete_laptop_data
    )

"""
Test insert statements:

INSERT INTO laptops (id, company, product, type_name, price_euros)
VALUES (1, 'Dell', 'Inspiron 3567', 'Notebook', 485);

INSERT INTO laptops (id, company, product, type_name, price_euros)
VALUES (2, 'HP', 'Inspiron 3566', 'Notebook', 900);

INSERT INTO laptops (id, company, product, type_name, price_euros)
VALUES (4, 'MacBook Pro', 'Some Fancy OS', 'Notebook', 950);
"""
