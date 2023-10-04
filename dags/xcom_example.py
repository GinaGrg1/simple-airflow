from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance

default_args = {
    'owner': 'rgurung'
}

def increment_by_1(value):
    print(f"Value is {value}!")
    return value + 1

def multiply_by_100(ti: TaskInstance) -> int:
    print(type(ti))
    value = ti.xcom_pull(task_ids='increment_by_1')
    print(f"Value before incremenet is: {value}!")

    return value * 100

def subtract_by_9(ti: TaskInstance) -> int:
    value = ti.xcom_pull(task_ids='multiply_by_100')

    return value - 9

with DAG(
    dag_id = 'cross_task_communication',
    description = 'Cross-task communication with XCom',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['xcom', 'python']
) as dag:
    
    increment_by_1 = PythonOperator(
        task_id = 'increment_by_1',
        python_callable = increment_by_1,
        op_kwargs = {'value': 1}
    )

    multiply_by_100 = PythonOperator(
        task_id = 'multiply_by_100',
        python_callable = multiply_by_100
    )

    subtract_by_9 = PythonOperator(
        task_id = 'subtract_by_9',
        python_callable = subtract_by_9
    )

increment_by_1 >> multiply_by_100 >> subtract_by_9
