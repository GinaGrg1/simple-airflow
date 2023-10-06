# simple-airflow

```
$ pip3 install "apache-airflow[celery]==2.5.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-3.7.txt"


$ pip3 install psycopg2-binary


$ pip3 install apache-airflow-providers-postgres  -- This is needed to be able to make connection to postgres from the UI.
```

Create a user:
```
$ airflow users create -e reginagurung@yahoo.co.uk -f Regina -l Gurung -p password -r Admin -u rgurung
```

Start Scheduler:
```
$ airflow scheduler
```

Start webserver
```
$ airflow webserver
```

To change the metadata db from SQLite to Postgres, make changes in the airflow.cfg file:
```
[core]
executor = LocalExecutor

[database]
# The SqlAlchemy connection string to the metadata database.
# SqlAlchemy supports many different database engine, more information
# their website.
# Create airflow_db
sql_alchemy_conn = postgresql://username:password@localhost:5432/airflow_db

```

Initialize airflow db
`$ airflow db init` and create new user


