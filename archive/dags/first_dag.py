from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

from pprint import pprint as pp

default_args = {
    'owner': 'rgurung'
}

def simple(**kwargs):
    pp(kwargs)

def simple_1(ti):
    pp(dir(ti))

with DAG(
    dag_id = 'first_dag',
    description = 'Creating the very first dag',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@daily',
    tags = ['beginner', 'kwargs', 'first dag']
) as dag:
    
    task1 = PythonOperator(
        task_id = 'first_dag_task',
        python_callable = simple
    )

    task2 = PythonOperator(
        task_id = 'second_dag_task',
        python_callable = simple_1
    )

chain(
task1,
task2
)
