import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from airflow.utils.dates import days_ago

from airflow.decorators import dag
from airflow.models.baseoperator import chain

import tasks as ts

default_args = {
    'owner': 'rgurung'
}

@dag(
    dag_id = 'second_dag',
    description = 'Creating the second dag',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['beginner', 'kwargs', 'second dag']
)
def second_dag():
    
    first_dag = ts.first_dag()
    second_dag_1 = ts.second_dag_1()
    
    chain(
    first_dag,
    second_dag_1
    )

second_dag()
