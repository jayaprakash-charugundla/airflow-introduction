from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

@task.python
def process_a():
    ti = get_current_context()['ti']
    print('Method process_a, value: ', ti.xcom_pull(key = 'pet_name', task_ids = 'extract', dag_id = 'my_dag'))
    print('Method process_a, value: ', ti.xcom_pull(key = 'pet_type', task_ids = 'extract', dag_id = 'my_dag'))

@task.python
def process_b():
    ti = get_current_context()['ti']
    print('Method process_b, value: ', ti.xcom_pull(key = 'pet_name', task_ids = 'extract', dag_id = 'my_dag'))
    print('Method process_b, value: ', ti.xcom_pull(key = 'pet_type', task_ids = 'extract', dag_id = 'my_dag'))

@task.python
def process_c():
    ti = get_current_context()['ti']
    print('Method process_c, value: ', ti.xcom_pull(key = 'pet_name', task_ids = 'extract', dag_id = 'my_dag'))
    print('Method process_c, value: ', ti.xcom_pull(key = 'pet_type', task_ids = 'extract', dag_id = 'my_dag'))

@task.python
def check_a():
    print("checking a")

@task.python
def check_b():
    print("checking b")

@task.python
def check_c():
    print("checking c")

def process_tasks():
    with TaskGroup(group_id = 'process_tasks') as process_tasks:
        process_a()
        process_b()
        process_c()
        with TaskGroup(group_id = 'test_tasks') as test_tasks:
            check_a()
            check_b()
            check_c()
    return process_tasks