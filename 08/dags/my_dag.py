from datetime import datetime, timedelta

from airflow.decorators import task, dag

@task.python
def extract():
   pet_name = "Max"
   pet_type = "Dog"
   return {"pet_name":pet_name, "pet_type":pet_type}

@task.python
def process(pet_json):
	print(pet_json['pet_name'])
	print(pet_json['pet_type'])

@dag(description = "DAG in charge of processing customer",
         start_date = datetime(2021, 1, 1),
         schedule_interval = "@daily",
         dagrun_timeout = timedelta(minutes = 10),
         tags = ["data_science", "customers"],
         catchup = False,
         max_active_runs = 1)
def my_dag():
    process(extract())

dag = my_dag()