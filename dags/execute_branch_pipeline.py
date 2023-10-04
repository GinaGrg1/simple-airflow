from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import TaskInstance

from random import choice

default_args = {
    'owner': 'rgurung'
}

def has_driving_license():
    return choice([True, False])

def branch(ti: TaskInstance):
    """
    The return values are function names that will be run.
    """
    if ti.xcom_pull(task_ids='has_driving_license'):
        return 'eligible_to_drive'
    return 'not_eligible_to_drive'

def eligible_to_drive():
    print("You can drive, you have a license")

def not_eligible_to_drive():
    print("I'm afraid you're not eligible to drive")

with DAG(
    dag_id = 'executing_branching',
    description = 'Branching pipelines run.',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'license']
) as dag:
    
    taskA = PythonOperator(
        task_id = 'has_driving_license',
        python_callable = has_driving_license
    )

    taskB = BranchPythonOperator(
        task_id = 'branch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'eligible_to_drive',
        python_callable = eligible_to_drive
    )

    taskD = PythonOperator(
        task_id = 'not_eligible_to_drive',
        python_callable = not_eligible_to_drive
    )

taskA >> taskB >> [taskC, taskD]
