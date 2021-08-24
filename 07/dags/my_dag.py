from airflow import DAG

from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def _extract(ti):
   pet_name = "Max"
   pet_type = "Dog"
   return {"pet_name":pet_name, "pet_type":pet_type}

def _process(ti):
    pet_json = ti.xcom_pull(task_ids = "extract")
	print(pet_json['pet_name'])
	print(pet_json['pet_type'])

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

	delete_from_pet_table = PostgresOperator(
		task_id = "delete_from_pet_table",
		postgres_conn_id = "postgres_default",
		sql = "DELETE from pet"
	)

	populate_pet_table = PostgresOperator(
		task_id = "populate_pet_table",
		postgres_conn_id = "postgres_default",
		sql = "sql/populate_data_into_pet.sql"
	)

	extract = PythonOperator(
        task_id = "extract",
        python_callable = _extract
        #op_args = ["{{var.value.my_dag_partner.name}}"]
    )

	process = PythonOperator(
        task_id = "process",
        python_callable = _process
    )

	fetch_pet_names_by_date = PostgresOperator(
		task_id = "fetch_pet_names_by_date",
		postgres_conn_id = "postgres_default",
		sql= "sql/fetch_pet_names_by_date.sql"
	)

	fetch_pet_names_by_name = PostgresOperator(
		task_id = "fetch_pet_names_by_name",
		postgres_conn_id = "postgres_default",
		sql= "sql/fetch_pet_names_by_name.sql",
		params={'pet_name': 'Max'}
	)

	#TODO Can't use xcom value in PostgresOperator
	fetch_pet_names_by_name_using_xcom = PostgresOperator(
		task_id = "fetch_pet_names_by_name_using_xcom",
		postgres_conn_id = "postgres_default",
		sql= "sql/fetch_pet_names_by_name.sql",
		params={'pet_name': '{{pet_name}}'}
	)


create_pet_table >> delete_from_pet_table >> populate_pet_table >> extract >> process >> fetch_pet_names_by_date >> fetch_pet_names_by_name >> fetch_pet_names_by_name_using_xcom