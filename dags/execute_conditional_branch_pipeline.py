import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable

from get_insurance_data import (
    read_csv_file, remove_null_values, groupby_smoker, filter_by_southwest, 
    filter_by_southeast, filter_by_northeast, filter_by_northwest)

default_args = {
    'owner': 'rgurung'
}

def determine_branch():
    """
    This variable/key 'transform_action' is created under Admin/Variables from the UI.
    """
    transform_action = Variable.get("transform_action", None)

    if transform_action.startswith("filter"):
        return transform_action
    elif transform_action == "groupby_region_smoker":
        return "groupby_region_smoker"

with DAG(
    dag_id = 'executing_conditional_branching',
    description = 'Conditional branching pipelines run.',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['branching', 'regions', 'conditions']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id = 'determine_branch',
        python_callable = determine_branch
    )

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

    groupby_smoker = PythonOperator(
        task_id = 'groupby_smoker',
        python_callable = groupby_smoker
    )

read_csv_file >> remove_null_values >> determine_branch >> [
    filter_by_southwest,
    filter_by_southeast,
    filter_by_northeast,
    filter_by_northwest
]
