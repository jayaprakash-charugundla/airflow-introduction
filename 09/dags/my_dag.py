from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import task, dag
from airflow.operators.python import get_current_context


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
    print('Method process, value: ', pet_json)
    print('Method process, value: ', type(pet_json))


# print('pet_name- ', pet_json["pet_name"])
# print('pet_type- ', pet_json["pet_type"])

@task.python
def process_pet_values(pet_name, pet_type):
    print(pet_name)
    print(type(pet_name))
    print(pet_type)
    print(type(pet_type))


@task.python
def process_a():
    ti = get_current_context()['ti']
    print('Method process_a, value: ',
          ti.xcom_pull(key='pet_name', task_ids='extract_names_xcom_push_disabled', dag_id='my_dag'))
    print('Method process_a, value: ',
          ti.xcom_pull(key='pet_type', task_ids='extract_names_xcom_push_disabled', dag_id='my_dag'))


@task.python
def process_b():
    ti = get_current_context()['ti']
    print('Method process_b, value: ',
          ti.xcom_pull(key='pet_name', task_ids='extract_names_xcom_push_disabled', dag_id='my_dag'))
    print('Method process_b, value: ',
          ti.xcom_pull(key='pet_type', task_ids='extract_names_xcom_push_disabled', dag_id='my_dag'))


@task.python
def process_c():
    ti = get_current_context()['ti']
    print('Method process_c, value: ',
          ti.xcom_pull(key='pet_name', task_ids='extract_names_xcom_push_disabled', dag_id='my_dag'))
    print('Method process_c, value: ',
          ti.xcom_pull(key='pet_type', task_ids='extract_names_xcom_push_disabled', dag_id='my_dag'))


@dag(description="DAG in charge of processing customer",
     start_date=datetime(2021, 1, 1),
     schedule_interval="@daily",
     dagrun_timeout=timedelta(minutes=10),
     tags=["data_science", "customers"],
     catchup=False,
     max_active_runs=1)
def my_dag():
    # pet_json = extract_names()
    # process(pet_json)

    # pet_json_dict = extract_names_dict()
    # process(pet_json_dict)
    # process_pet_name(pet_json_dict["pet_name"])

    # TODO process_pet not working
    # process_pet(pet_json["pet_name"], pet_json["pet_type"])

    # extract_names() >> process_a()
    extract_names_xcom_push_disabled() >> process_a() >> process_b() >> process_c()


dag = my_dag()
