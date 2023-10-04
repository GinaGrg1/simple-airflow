from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'rgurung'
}

def print_function():
    print("Testing Python Operator.")

with DAG(
    dag_id = 'executing_python_operators',
    description = 'Python operator in DAGs.',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['simple', 'python']
) as dag:
    
    task = PythonOperator(
        task_id = 'Python_task_A',
        python_callable = print_function
    )

task
