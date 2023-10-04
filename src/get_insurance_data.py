import pandas as pd
import os

from airflow.models import TaskInstance

os.chdir('..')

def read_csv():
    df = pd.read_csv(f"{os.getcwd()}/datasets/insurance.csv")

    return df.to_json()

def remove_null_values(**kwargs):
    ti = kwargs.get('ti')

    json_data = ti.xcom_pull(task_ids='read_csv_file') # get the output of func read_csv
    df = pd.read_json(json_data)
    df = df.dropna()

    return df.to_json()

def groupby_smoker(ti: TaskInstance) -> None:
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    smoker_df.to_csv(f"{os.getcwd()}/output/grouped_by_smoker.csv", index=False)

def groupby_region(ti: TaskInstance) -> None:
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
    }).reset_index()

    region_df.to_csv(f"{os.getcwd()}/output/grouped_by_region.csv", index=False)
  
