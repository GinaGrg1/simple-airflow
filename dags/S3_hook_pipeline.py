# Tu run from the command line:
# $ airflow dags test S3_hook_pipeline 2023-10-05

from io import StringIO
import pandas as pd

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.S3_hook import S3Hook

default_args = {
    'owner': 'rgurung'
}

def read_s3_file(bucket_name, file_key):
    """
    Make the aws connection from the UI. Need to create access policies in AWS.
    """
    s3_hook = S3Hook(aws_conn_id='aws_conn_s3')
    file_content = s3_hook.read_key(bucket_name=bucket_name, key=file_key)

    if isinstance(file_content,bytes):
        file_content = file_content.decode('utf-8')

    df = pd.read_csv(StringIO(file_content))
    return df.to_json()

def remove_null_values(json_data):
    """
    This json_data will be pulled from XCom.
    """
    df = pd.read_json(json_data)
    df = df.dropna()

    return df.to_json()

def create_table_customer_credit_card_details():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS customer_credit_card_details (
            id INT,
            name VARCHAR(255),
            email VARCHAR(255),
            credit_card_number VARCHAR(50),
            credit_card_type VARCHAR(50)
        );
    """

    pg_hook.run(sql=create_table_query)

def insert_data_customer_credit_card_details(json_data):
    df = pd.read_json(json_data)

    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    for _, row in df.iterrows():
        insert_query = f"""
                INSERT INTO customer_credit_card_details
                (id, name, email, credit_card_number, credit_card_type)
                VALUES ({row['id']}, 
                        {row['name']}, 
                        {row['email']},
                        {row['credit_card_number']},
                        {row['credit_card_type']});
        """
        pg_hook.run(sql=insert_query)

with DAG(
    dag_id = 'S3_hook_pipeline',
    description = 'Executing pipeline with S3 hook',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['aws', 'S3', 'hooks']
) as dag:
    
    read_s3_file_task = PythonOperator(
        task_id = 'read_s3_file_task',
        python_callable = read_s3_file,
        op_kwargs = {
            'bucket_name': 'some_bucket_name',
            'file_key': 'credit_card_details.csv'
        }
    )

    remove_null_values_task =PythonOperator(
        task_id = 'remove_null_values_task',
        python_callable = remove_null_values,
        op_kwargs = {
            "json_data": "{{ ti.xcom_pull(task_ids='read_s3_file_task')}}"
        }
    )

    create_table_customer_credit_card_details_task = PythonOperator(
        task_id = 'create_table_customer_credit_card_details_task',
        python_callable = create_table_customer_credit_card_details,
        op_kwargs = {
            'json_data': '{{ ti.xcom_pull(task_ids="remove_null_values_task") }}'
        }
    )

    insert_data_customer_credit_card_details_task = PythonOperatorTask(
        task_id = 'insert_data_customer_credit_card_details_task',
        python_callable = create_table_customer_credit_card_details,
        op_kwargs = {
            'json_data': '{{ ti.xcom_pull(task_ids="remove_null_values_task") }}'
        }
    )

    chain(
        read_s3_file_task,
        remove_null_values_task,
        create_table_customer_credit_card_details_task,
        insert_data_customer_credit_card_details_task
    )
    
