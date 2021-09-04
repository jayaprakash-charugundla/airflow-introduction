import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(dag_id="print_task_context",
          start_date=airflow.utils.dates.days_ago(3),
          schedule_interval="@daily")


def _print_context(**context):
    print("Method _print_context::", context["execution_date"])


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag
)
