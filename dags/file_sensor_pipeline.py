from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

from pprint import pprint as pp
import pandas as pd

default_args = {
    'owner': 'rgurung',
    'email': ['someemail@yahoo.co.uk']
}

with DAG(
    dag_id = 'simple_file_sensor',
    description = 'Simple file sensor pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['file', 'filesensor', 'sensor']
    #params={'filename': Variable.get('file_name')} #access this by {{ params.filename }}
) as dag:
    
    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = f"/Users/reginagurung/airflow/tmp/{Variable.get('file_name')}",
        poke_interval = 10,
        timeout = 60 * 10
    )

    checking_for_file
