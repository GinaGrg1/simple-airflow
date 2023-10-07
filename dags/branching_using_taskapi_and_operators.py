import pandas as pd
from pathlib import Path

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'rgurung'
}

def read_csv_file():
    df = pd.read_csv(f"{Path.home()}/airflow/datasets/car_data.csv")
    return df.to_json()

@task.branch
def determine_branch():
    final_output = Variable.get("transform", default_var=None)

    if final_output == 'filter_two_seaters':
        return 'filter_two_seaters_task'
    elif final_output == 'filter_fwds':
        return 'filter_fwds_task'
        
def filter_two_seaters(ti: TaskInstance):
    """
    When this task is run, it exits with two keys transform_result & transform_filename.
    """
    json_data = ti.xcom_pull(task_ids='read_csv_file_task')

    df = pd.read_json(json_data)
    two_seater_df = df[df['Seats'] == 2]

    ti.xcom_push(key='transform_result', value=two_seater_df.to_json())
    ti.xcom_push(key='transform_filename', value='two_seaters')

def filter_fwds(ti: TaskInstance):
    """
    When this task is run, it exits with two keys transform_result & transform_filename.
    """
    json_data = ti.xcom_pull(task_ids='read_csv_file_task')

    df = pd.read_json(json_data)
    fwds_df = df[df['PowerTrain'] == 'FWD']

    ti.xcom_push(key='transform_result', value=fwds_df.to_json())
    ti.xcom_push(key='transform_filename', value='fwds')

def write_csv_result(ti: TaskInstance):
    json_data = ti.xcom_pull(key='transform_result')
    file_name = ti.xcom_pull(key='transform_filename')

    df = pd.read_json(json_data)
    df.to_csv(f"{Path.home()}/airflow/output/{file_name}.csv", index=False)

@dag(
    dag_id = 'branching_using_taskflow',
    description = 'Branching using taskflow.',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'taskapi']
)
def branching_using_taskflow():

    read_csv_file_task = PythonOperator(
        task_id = 'read_csv_file_task',
        python_callable = read_csv_file
    )

    filter_two_seaters_task = PythonOperator(
        task_id = 'filter_two_seaters_task',
        python_callable = filter_two_seaters
    )

    filter_fwds_task = PythonOperator(
        task_id = 'filter_fwds_task',
        python_callable = filter_fwds
    )

    write_csv_results_task = PythonOperator(
        task_id = 'write_csv_results_task',
        python_callable = write_csv_result,
        trigger_rule = 'none_failed'
    )

    read_csv_file_task >> determine_branch() >> [
        filter_two_seaters_task,
        filter_fwds_task
    ] >> write_csv_results_task

branching_using_taskflow()
