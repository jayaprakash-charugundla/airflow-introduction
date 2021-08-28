from datetime import datetime, timedelta
from airflow.decorators import task, dag
from groups.process_tasks import process_tasks

@task.python
def extract(task_id = "extract", do_xcom_push = False, multiple_outputs = True):
   pet_name = "Max"
   pet_type = "Dog"
   return {"pet_name":pet_name, "pet_type":pet_type}

@dag(description = "DAG in charge of processing customer",
         start_date = datetime(2021, 1, 1),
         schedule_interval = "@daily",
         dagrun_timeout = timedelta(minutes = 10),
         tags = ["data_science", "customers"],
         catchup = False,
         max_active_runs = 1)
def my_dag():
	extract() >> process_tasks()

dag = my_dag()