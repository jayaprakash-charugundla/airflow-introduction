from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from groups.process_tasks import process_tasks

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

@dag(description = "DAG in charge of processing customer",
         start_date = datetime(2021, 1, 1),
         schedule_interval = "@daily",
         dagrun_timeout = timedelta(minutes = 10),
         tags = ["data_science", "customers"],
         catchup = False,
         max_active_runs = 1)
def my_dag():
    start = DummyOperator(task_id = "start")

    for partner, details in partners.items():
        @task.python(task_id = f"extract_{partner}", priority_weight = details['priority'], pool = details['pool'],
        do_xcom_push = False, multiple_outputs = True)
        def extract(partner_name, partner_path):
           return {"partner_name":partner_name, "partner_path":partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values)

dag = my_dag()