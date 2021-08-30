from datetime import datetime
from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream

@dag(description = "DAG dependencies",
         start_date = datetime(2021, 1, 1),
         schedule_interval = "@daily",
         catchup = False)
def dag_dependencies():
    t1 = DummyOperator(task_id = "t1")
    t2 = DummyOperator(task_id = "t2")
    t3 = DummyOperator(task_id = "t3")
    t4 = DummyOperator(task_id = "t4")
    t5 = DummyOperator(task_id = "t5")
    t6 = DummyOperator(task_id = "t6")

    cross_downstream([t1, t2, t3], [t4, t5, t6])
    #chain(t1, [t2, t3], [t4, t5], t6)


dag = dag_dependencies()