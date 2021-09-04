from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1)
}

with DAG(dag_id="cleaning_dag",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:
    cleaning_xcoms = PostgresOperator(
        task_id='cleaning_xcoms',
        sql='sql/CLEANING_XCOMS.sql',
        postgres_conn_id='postgres'
    )

    waiting_for_task >> cleaning_xcoms
