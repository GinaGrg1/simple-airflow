import os
import pandas as pd

os.chdir('..')

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable, TaskInstance
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

default_args = {
    'owner': 'rgurung'
}

def read_csv_file(ti: TaskInstance):
    df = pd.read_csv(f"{os.getcwd()}/datasets/insurance.csv")

    ti.xcom_push(key='my_csv', value=df.to_json())

def remove_null_values(ti: TaskInstance):
    json_data = ti.xcom_pull(key='my_csv')
    df = pd.read_json(json_data)
    df = df.dropna()

    ti.xcom_push(key='my_clean_csv', value=df.to_json())

def determine_branch():
    """
    This variable/key 'transform_action' is created under Admin/Variables from the UI.
    """
    transform_action = Variable.get("transform_action", None)

    if transform_action.startswith("filter"):
        return f"filtering.{transform_action}"
    elif transform_action == "groupby_region_smoker":
        return f"grouping.{transform_action}"
    
def filter_by_southwest(ti: TaskInstance):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']
    region_df.to_csv(f"{os.getcwd()}/output/southwest.csv", index=False)

def filter_by_southeast(ti: TaskInstance):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']
    region_df.to_csv(f"{os.getcwd()}/output/southeast.csv", index=False)

def filter_by_northeast(ti: TaskInstance):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']
    region_df.to_csv(f"{os.getcwd()}/output/northeast.csv", index=False)

def filter_by_northwest(ti: TaskInstance):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']
    region_df.to_csv(f"{os.getcwd()}/output/northwest.csv", index=False)

def groupby_region_smoker(ti: TaskInstance):
    json_data = ti.xcom_pull(key='my_clean_csv')
    df = pd.read_json(json_data)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(f"{os.getcwd()}/output/grouped_by_smoker.csv", index=False)

with DAG(
    dag_id = 'taskgroups_and_edgelabels',
    description = 'Branching pipelines with TaskGroups & EdgeLabels',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'regions', 'edgelabels', 'taskgroups']
    ) as dag:

    with TaskGroup('reading_and_preprocessing') as reading_and_preprocessing:
        read_csv_file = PythonOperator(
            task_id = 'read_csv_file',
            python_callable = read_csv_file
        )

        remove_null_values = PythonOperator(
            task_id = 'remove_null_values',
            python_callable = remove_null_values
        )

        read_csv_file >> remove_null_values

    determine_branch = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable = determine_branch
    )

    with TaskGroup('filtering') as filtering:
        filter_by_southwest = PythonOperator(
            task_id = 'filter_by_southwest',
            python_callable = filter_by_southwest
        )

        filter_by_southeast = PythonOperator(
            task_id = 'filter_by_southeast',
            python_callable = filter_by_southeast
        )

        filter_by_northeast = PythonOperator(
            task_id = 'filter_by_northeast',
            python_callable = filter_by_northeast
        )

        filter_by_northwest = PythonOperator(
            task_id = 'filter_by_northwest',
            python_callable = filter_by_northwest
        )

    with TaskGroup('grouping') as grouping:
        groupby_smoker = PythonOperator(
            task_id = 'groupby_region_smoker',
            python_callable = groupby_region_smoker
        )

    reading_and_preprocessing >> Label('preprocessed data') >> determine_branch >> Label('branch on condition') >> [filtering, grouping]
