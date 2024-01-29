"""
Пример подключения по SSH:
 - таск подключается по параметрам указанным в подключении `test_ssh` и выполняет простейшую команду
"""
from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'airflow',
    'description': 'Test SSH',
    'depend_on_past': False,
    'start_date': datetime(2019, 6, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'df_test_ssh'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_DAG_ID': dag_id,  # обязательно наличие в env, для загрузки метаинформации о выполняемых задачах
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_JOB_ID': '{{ ti.job_id }}'
               }


def run_ssh(task_id, conn_id, cmd):
    return SSHOperator(
        task_id=task_id,
        ssh_conn_id=conn_id,
        command=cmd)


def run_fake(task_id):
    return DummyOperator(task_id=task_id)


with DAG(dag_id,
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['test']) as dag:
    t_start = run_fake(f'{dag_id}.start')
    t_ssh = run_ssh(f'{dag_id}.ssh', 'test_ssh', 'ls -l')
    t_end = run_fake(f'{dag_id}.end')

    t_start >> t_ssh >> t_end

    dag.doc_md = __doc__