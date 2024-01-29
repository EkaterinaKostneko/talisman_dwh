"""
"""
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


@task(task_id="run_this")
def run_this_func(dag_run=None):
    """
    Print the payload "message" passed to the DagRun conf attribute.
    :param dag_run: The DagRun object
    :type dag_run: DagRun
    """
    print(f"Remotely received value of {dag_run.conf.get('message')} for key=message")


def find_str(file_path, pattern):
    """
    Поиск строк, реализация в зависимости от решаемых задач
    """
    import re

    my_regex = re.compile(pattern)
    found_strs = set()
    for line in open(file_path):
        match = my_regex.match(line)
        if match:
            found_strs.add(match.group(0))

    s = None
    if found_strs:
        s = '\n,'.join(found_strs)
    return s


def get_task_log_path(dag_id, task_id):
    """
    TODO
    """
    return f'home/airflow/logs/{dag_id}/{task_id}/2022-04-22T10:14:34.378446+00:00/1.log'


DAG_ID = 'df_example_observer_body'
state = 'wa'

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=['example'],
) as dag:
    run_this = run_this_func()

    bash_task = BashOperator(
        task_id=f"{DAG_ID}.bash_task",
        bash_command='echo "Here is the message: $message"',
        env={'message': '{{ dag_run.conf.get("message") }}'},
    )
