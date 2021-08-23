from airflow import DAG

from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def _extract(partner_name):
    print(partner_name)

with DAG("my_dag",
         description = "DAG in charge of processing customer",
         start_date = datetime(2021, 1, 1),
         schedule_interval = "@daily",
         dagrun_timeout = timedelta(minutes = 10),
         tags = ["data_science", "customers"],
         catchup = False,
         max_active_runs = 1) as dag:

	create_pet_table = PostgresOperator(
		task_id = "create_pet_table",
		postgres_conn_id = "postgres_default",
		sql = "sql/create_pet.sql"
	)

	populate_pet_table = PostgresOperator(
		task_id = "populate_pet_table",
		postgres_conn_id = "postgres_default",
		sql = "sql/insert_data_into_pet.sql"
	)

	fetch_pet_names = PostgresOperator(
		task_id = "fetch_pet_names",
		postgres_conn_id = "postgres_default",
		sql= "sql/fetch_pet_names.sql"
	)


create_pet_table >> populate_pet_table >> fetch_pet_names