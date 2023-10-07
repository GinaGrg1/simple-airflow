from pathlib import Path
import pandas as pd
import psycopg2
import glob

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'rgurung'
}

FILEPATH = f"{Path.home()}/airflow/tmp/{Variable.get('file_name')}"  # laptops_*.csv
FILE_COLS = ['Id', 'Company', 'Product', 'TypeName', 'Price_euros']

OUTPUT_FILE_PATH = f"{Path.home()}/airflow/output/"

def insert_laptop_data():
    conn = psycopg2.connect(host='localhost', database='customers_db', user='postgres', password='regina')
    
    with conn.cursor() as cur:
        for file in glob.glob(FILEPATH):
            df = pd.read_csv(file, usecols=FILE_COLS)

            records = df.to_dict('records')
            for record in records:
                query = f"""INSERT INTO laptops
                            (id, company, product, type_name, price_euros)
                            VALUES (
                                {record['Id']},
                                '{record['Company']}',
                                '{record['Product']}',
                                '{record['TypeName']}',
                                '{record['Price_euros']}'
                            )
                        """
                print('this is the query',  query)
                cur.execute(query)
        conn.commit()
        conn.close()

def gaming_laptops():
    for file in glob.glob(FILEPATH):
        df = pd.read_csv(file, usecols=FILE_COLS)

        gaming_laptops_df = df[df['TypeName'] == 'Gaming']
        gaming_laptops_df.to_csv(f"{OUTPUT_FILE_PATH}/gaming_laptops.csv", mode='a', header=False, index=False)

def notebook_laptops():
    for file in glob.glob(FILEPATH):
        df = pd.read_csv(file, usecols=FILE_COLS)

        gaming_laptops_df = df[df['TypeName'] == 'Notebook']
        gaming_laptops_df.to_csv(f"{OUTPUT_FILE_PATH}/notebook_laptops.csv", mode='a', header=False, index=False)

def ultrabook_laptops():
    for file in glob.glob(FILEPATH):
        df = pd.read_csv(file, usecols=FILE_COLS)

        gaming_laptops_df = df[df['TypeName'] == 'Ultrabook']
        gaming_laptops_df.to_csv(f"{OUTPUT_FILE_PATH}/ultrabook_laptops.csv", mode='a', header=False, index=False)

with DAG(
    dag_id = 'pipeline_postgres_with_filesensor',
    description = 'Executing pipeline with Filesensor Postgres db',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['postgres', 'filesensor', 'sensor'],
    template_searchpath = f"{Path.home()}/airflow/sql_statements"
) as dag:
    
    create_table_laptop = PostgresOperator(
        task_id = 'create_table_laptop',
        postgres_conn_id = 'postgres_conn',
        sql = 'create_table_laptops.sql',
    )

    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = FILEPATH,
        poke_interval = 10,
        timeout = 60 * 10
    )

    insert_laptop_data = PythonOperator(
        task_id = 'insert_laptop_data',
        python_callable = insert_laptop_data
    )

    filter_gaming_laptop = PythonOperator(
        task_id = 'filter_gaming_notebook',
        python_callable = gaming_laptops
    )

    filter_notebook_laptop = PythonOperator(
        task_id = 'filter_notebook_laptop',
        python_callable = notebook_laptops
    )

    filter_ultrabook_laptop = PythonOperator(
        task_id = 'filter_ultrabook_laptop',
        python_callable = ultrabook_laptops
    )

    delete_file = BashOperator(
        task_id = 'delete_file',
        bash_command = f'rm {FILEPATH}'
    )

    chain(
        create_table_laptop,
        checking_for_file,
        insert_laptop_data,
        [filter_gaming_laptop, filter_notebook_laptop, filter_ultrabook_laptop],
        delete_file
    )
