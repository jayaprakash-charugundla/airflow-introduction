from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from groups.process_tasks import process_tasks
import time

partners = {
    "partner_snowflake": {
        "name": "snowflake",
        "path": "/partners/snowflake",
        "priority": 2,
        "pool": "snowflake"
    },
    "partner_netflix": {
        "name": "netflix",
        "path": "/partners/netflix",
        "priority": 3,
        "pool": "netflix"
    },
    "partner_astronomer": {
        "name": "astronomer",
        "path": "/partners/astronomer",
        "priority": 1,
        "pool": "astronomer"
    }
}

default_args = {
	"start_date" : datetime(2021, 1, 1),
	"retries": 0
}

def _success_callback(context):
    print(context)

def _failure_callback(context):
    print(context)

def _extract_success_callback(context):
    print('SUCCESS CALLBACK')

def _extract_failure_callback(context):
    print('FAIL CALLBACK')

def _extract_retry_callback(context):
    print('RETRY CALLBACK')

@dag(description = "DAG",
         default_args = default_args,
         schedule_interval = "@daily",
         dagrun_timeout = timedelta(minutes = 10),
         tags = ["data_science", "customers"],
         catchup = False,
         max_active_runs = 1,
         on_success_callback = _success_callback,
         on_failure_callback = _failure_callback)
def my_dag():
    start = DummyOperator(task_id = "start")

    for partner, details in partners.items():
        @task.python(task_id = f"extract_{partner}",
            on_success_callback = _extract_success_callback,
            on_failure_callback = _extract_failure_callback,
            on_retry_callback = _extract_retry_callback,
            retries = 3,
            retry_delay = timedelta(minutes = 5),
            retry_exponential_backoff = True,
            max_retry_delay = timedelta(minutes = 15),
            priority_weight = details['priority'],
            pool = details['pool'],
            do_xcom_push = False,
            multiple_outputs = True)
        def extract(partner_name, partner_path):
           time.sleep(3)
           return {"partner_name":partner_name, "partner_path":partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values)

dag = my_dag()