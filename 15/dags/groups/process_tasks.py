from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


@task.python
def process_a(partner_name, partner_path):
    print("process_a :: ", partner_name)
    print("process_a :: ", partner_path)


@task.python
def process_b(partner_name, partner_path):
    print("process_b :: ", partner_name)
    print("process_b :: ", partner_path)


@task.python
def process_c(partner_name, partner_path):
    print("process_c :: ", partner_name)
    print("process_c :: ", partner_path)


def process_tasks(partner_settings):
    with TaskGroup(group_id='process_tasks', add_suffix_on_collision=True) as process_tasks:
        process_a(partner_settings['partner_name'], partner_settings['partner_path'])
        process_b(partner_settings['partner_name'], partner_settings['partner_path'])
        process_c(partner_settings['partner_name'], partner_settings['partner_path'])
    return process_tasks
