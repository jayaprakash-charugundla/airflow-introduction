from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import task, dag


@task.python
def extract():
    pet_name = "Max"
    pet_type = "Dog"
    return {"pet_name": pet_name, "pet_type": pet_type}


@task.python(task_id="extract_names_xcom_push_disabled", do_xcom_push=False, multiple_outputs=True)
def extract_names_xcom_push_disabled():
    pet_name = "Max"
    pet_type = "Dog"
    return {"pet_name": pet_name, "pet_type": pet_type}


@task.python(task_id="extract_names", multiple_outputs=True)
def extract_names():
    pet_name = "Max"
    pet_type = "Dog"
    return {"pet_name": pet_name, "pet_type": pet_type}


@task.python(task_id="extract_names_dict")
def extract_names_dict() -> Dict[str, str]:
    pet_name = "Max"
    pet_type = "Dog"
    return {"pet_name": pet_name, "pet_type": pet_type}


@task.python
def process(pet_json):
    # print(pet_json['pet_name'])
    # print(pet_json['pet_type'])
    print(pet_json)


@task.python
def process_pet(pet_name, pet_type):
    print(pet_name)
    print(pet_type)


@dag(description="DAG in charge of processing customer",
     start_date=datetime(2021, 1, 1),
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=10),
     tags=["data_science", "customers"],
     catchup=False,
     max_active_runs=1)
def my_dag():
    pet_json = extract_names()
    process(pet_json)


# pet_json_dict = extract_names_dict()
# process(pet_json_dict)
# TODO process_pet not working
# process_pet(pet_json['pet_name'], pet_json['pet_type'])

dag = my_dag()
