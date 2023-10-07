import pandas as pd
from pathlib import Path

from airflow.operators.sqlite_operator import SqliteOperator

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    'owner': 'rgurung'
}

@dag(dag_id='data_transformation_storage_pipeline',
    description = 'Data transformation and storage pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['sqlite', 'python', 'taskflow_api', 'xcoms'])
def data_transformation_storage_pipeline():
    # by default the task_id is the name of the function when using task api.
    @task
    def read_car_data():
        df = pd.read_csv(f'{Path.home()}/airflow/datasets/car_data.csv')
        return df.to_json()
    
    @task
    def read_car_categories():
        df = pd.read_csv(f'{Path.home()}/airflow/datasets/car_categories.csv')        
        return df.to_json()

    @task
    def create_table_car_data():
        sqlite_operator = SqliteOperator(
            task_id="create_table_car_data",
            sqlite_conn_id="my_sqlite_conn",
            sql="""CREATE TABLE IF NOT EXISTS car_data (
                        id INTEGER PRIMARY KEY,
                        brand TEXT NOT NULL,
                        model TEXT NOT NULL,
                        body_style TEXT NOT NULL,
                        seat INTEGER NOT NULL,
                        price INTEGER NOT NULL);""",
        )

        sqlite_operator.execute(context=None)

    @task
    def create_table_car_categories():
        sqlite_operator = SqliteOperator(
            task_id="create_table_car_categories",
            sqlite_conn_id="my_sqlite_conn",
            sql="""CREATE TABLE IF NOT EXISTS car_categories (
                        id INTEGER PRIMARY KEY,
                        brand TEXT NOT NULL,
                        category TEXT NOT NULL);""",
        )
        sqlite_operator.execute(context=None)

    @task
    def insert_car_data(**kwargs):
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='read_car_data')
        
        df = pd.read_json(json_data)     
        df = df[['Brand', 'Model', 'BodyStyle', 'Seats', 'PriceEuro']]      
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        insert_query = """
            INSERT INTO car_data (brand, model, body_style, seat, price)
            VALUES (?, ?, ?, ?, ?)
        """

        parameters = df.to_dict(orient='records')

        for record in parameters:
            sqlite_operator = SqliteOperator(
                task_id="insert_car_data",
                sqlite_conn_id="my_sqlite_conn",
                sql=insert_query,
                parameters=tuple(record.values()),
            )

            sqlite_operator.execute(context=None)

    @task
    def insert_car_categories(**kwargs):
        ti = kwargs['ti']
        
        json_data = ti.xcom_pull(task_ids='read_car_categories')       
        df = pd.read_json(json_data)       
        df = df[['Brand', 'Category']]      
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        insert_query = """
            INSERT INTO car_categories (brand, category)
            VALUES (?, ?)
        """

        parameters = df.to_dict(orient='records')

        for record in parameters:
            sqlite_operator = SqliteOperator(
                task_id="insert_car_categories",
                sqlite_conn_id="my_sqlite_conn",
                sql=insert_query,
                parameters=tuple(record.values()),
            )

            sqlite_operator.execute(context=None)

    @task
    def join():
        sqlite_operator = SqliteOperator(
            task_id="join_table",
            sqlite_conn_id="my_sqlite_conn",
            sql="""CREATE TABLE IF NOT EXISTS car_details AS
                   SELECT car_data.brand, 
                          car_data.model, 
                          car_data.price, 
                          car_categories.category
                    FROM car_data JOIN car_categories 
                    ON car_data.brand = car_categories.brand;
                """,
        )

        sqlite_operator.execute(context=None)

    join_task = join()

    read_car_data() >> create_table_car_data() >> \
        insert_car_data() >> join_task
    read_car_categories() >> create_table_car_categories() >> \
        insert_car_categories() >> join_task

data_transformation_storage_pipeline()
