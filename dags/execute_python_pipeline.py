import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

import get_insurance_data as ins

default_args = {
    'owner': 'rgurung'
}

with DAG(
    dag_id = 'python_pipeline',
    description = 'Running a Python pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'python', 'pandas']
) as dag:
    
    read_csv_file = PythonOperator(
        task_id = 'read_csv_file',
        python_callable = ins.read_csv
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = ins.remove_null_values
    )

    groupby_smoker = PythonOperator(
        task_id = 'groupby_smoker',
        python_callable = ins.groupby_smoker
    )

    groupby_region = PythonOperator(
        task_id = 'groupby_region',
        python_callable = ins.groupby_region
    )

chain(
read_csv_file,
remove_null_values,
[groupby_smoker, groupby_region]
)
