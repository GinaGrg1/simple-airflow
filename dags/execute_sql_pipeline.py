from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.sqlite_operator import SqliteOperator

default_args = {
    'owner': 'rgurung'
}

with DAG(
    dag_id = 'sqlite_pipeline',
    description = 'Running a pipeline using SQL operators.',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sqlite']
) as dag:
    create_table = SqliteOperator(
        task_id = 'create_sqlite_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users1 (
                name VARCHAR(50) NOT NULL,
                age INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT true
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn', # created from UI under Admin.
        dag = dag)
    
    insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users1 (name, age, is_active) VALUES
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Jing', 37, false),
                ('Irene', 54, false),
                ('Joseph', 27, true);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag)
    
    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users1 (name, age) VALUES
                ('Regina', 37),
                ('Sabina', 39),
                ('Jayshan', 37),
                ('Laxmi', 54),
                ('Om', 68);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag)
    
    display_result = SqliteOperator(
        task_id = 'display_result',
        sql = r"""SELECT * FROM users1""",
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
        do_xcom_push = True # push the result to XCom
    )

create_table >> [insert_values_1, insert_values_2] >> display_result
