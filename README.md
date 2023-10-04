# simple-airflow

```
$ pip3 install "apache-airflow[celery]==2.5.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-3.7.txt"
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
