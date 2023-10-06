from pprint import pprint as pp
from airflow.decorators import task

@task
def first_dag(**kwargs):
    pp(kwargs.keys())

@task
def second_dag_1(**kwargs):
    ti = kwargs.get('ti')
    pp(dir(ti))
