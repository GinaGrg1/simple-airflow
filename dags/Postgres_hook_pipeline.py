from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'rgurung'
}

employees_table = Variable.get("emp", default_var=None)
departments_table = Variable.get("dept", default_var=None)
employees_departments_table = Variable.get("emp_dept", default_var=None)

def create_employees_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {employees_table} (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            age INTEGER NOT NULL,
            department_id INTEGER NOT NULL);
        """
    
    pg_hook.run(create_table_query)

def create_department_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {departments_table} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL);
        """
    pg_hook.run(create_table_query)

def insert_data_employees(employees: list):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    insert_data_query = f"""
        INSERT INTO {employees_table} (first_name, last_name, age, department_id)
        VALUES (%s, %s, %s, %s)
    """

    for employee in employees:
        first_name, last_name, age, department_id = employee

        pg_hook.run(insert_data_query, parameters=(first_name, last_name, age, department_id))

def insert_data_departments(departments: list):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    insert_data_query = f"""
        INSERT INTO {departments_table} (name) VALUES (%s)
    """

    for department in departments:
        name= department

        pg_hook.run(insert_data_query, parameters=(name,))

def join_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    join_table_query = f"""
        CREATE TABLE IF NOT EXISTS {employees_departments_table} AS
        SELECT
            employees.first_name,
            employees.last_name,
            employees.age,
            departments.name AS department_name
        FROM employees JOIN departments
        ON employees.department_id = departments.id
        """
    pg_hook.run(join_table_query)

def display_emp_dept():
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    retrieve_result_query = f"SELECT * FROM {employees_departments_table}"
    results = pg_hook.get_records(retrieve_result_query)

    for row in results:
        print(row)

def filtering_join_table(condition):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')

    retrieve_result_query = f"SELECT * FROM {employees_departments_table} WHERE department_name='{condition}'"
    results = pg_hook.get_records(retrieve_result_query)

    for row in results:
        print(row)

with DAG(
    dag_id = 'postgres_hook_pipeline',
    description = 'Running a pipeline using the PostgresHook',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'postgreshook']
) as dag:
    
    create_employees_table_task = PythonOperator(
        task_id = 'create_employees_table',
        python_callable = create_employees_table
    )

    create_department_table_task = PythonOperator(
        task_id = 'create_department_table',
        python_callable = create_department_table
    )

    employees = [
        ('Sabina', 'Shrestha', 25, 1),
        ('Regina', 'Gurung', 33, 1),
        ('Ajay', 'Patel', 27, 2),
        ('Jayshan', 'Raghunandan', 26, 2),
        ('Ryan', 'Gosling', 30, 1),
        ('Brad', 'Pitt', 43, 3)
    ]

    insert_data_employees_task = PythonOperator(
        task_id = 'insert_data_employees',
        python_callable = insert_data_employees,
        op_kwargs = {
            'employees': employees
        }
    )

    departments = ['Engineering', 'Marketing', 'Sales']

    insert_data_departments_task = PythonOperator(
        task_id = 'insert_data_departments',
        python_callable = insert_data_departments,
        op_kwargs = {
            'departments': departments
        }
    )

    join_table_task = PythonOperator(
        task_id = 'join_table',
        python_callable = join_table
    )

    display_emp_dept_task = PythonOperator(
        task_id = 'display_emp_dept',
        python_callable = display_emp_dept
    )

    filtering_join_table_task = PythonOperator(
        task_id = 'filtering_join_table',
        python_callable = filtering_join_table,
        op_kwargs = {
            'condition': 'Engineering'
        }
    )

    create_employees_table_task >> insert_data_employees_task >> \
        join_table_task >> display_emp_dept_task >> filtering_join_table_task
    
    create_department_table_task >> insert_data_departments_task >> \
        join_table_task >> display_emp_dept_task >> filtering_join_table_task
