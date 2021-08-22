from airflow import DAG

from datetime import datetime, timedelta

with DAG("my_dag",
        description = "DAG in charge of processing customer",
        start_date = datetime(2021, 1, 1),
        schedule_interval = "@daily",
        dagrun_timeout = timedelta(minutes = 10),
        tags = ["data_science", "customers"],
        catchup = False) as dag:
        None
