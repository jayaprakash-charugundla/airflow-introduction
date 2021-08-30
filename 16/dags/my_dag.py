from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.sensors.date_time import DateTimeSensor
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
	"start_date" : datetime(2021, 1, 1)
}

@dag(description = "DAG",
         default_args = default_args,
         schedule_interval = "@daily",
         dagrun_timeout = timedelta(minutes = 10),
         tags = ["data_science", "customers"],
         catchup = False,
         max_active_runs = 1)
def my_dag():
    start = DummyOperator(task_id = "start")

    delay = DateTimeSensor(
        task_id = "delay",
        target_time = "{{execution_date.add(hours = 9)}}",
        poke_interval = 60 * 60,
        #mode = "poke"
        mode = "reschedule",
        timeout = 60 * 60 * 10
        #execution_timeout = 60 * 60,
        #soft_fail = True,
        #exponential_backoff = True
    )

    for partner, details in partners.items():
        @task.python(task_id = f"extract_{partner}", priority_weight = details['priority'], pool = details['pool'],
        do_xcom_push = False, multiple_outputs = True, execution_timeout = timedelta(minutes = 10))
        def extract(partner_name, partner_path):
           time.sleep(3)
           return {"partner_name":partner_name, "partner_path":partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values)

dag = my_dag()