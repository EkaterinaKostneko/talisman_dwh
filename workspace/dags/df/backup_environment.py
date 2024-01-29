'''
Created on 03 Jul 2019

@author: pymancer
'''
import os

from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner':            'admin',
    'description':      'Airflow Environment Backup',
    'depend_on_past':   False,
    'start_date':       datetime(2019, 7, 3),
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          0
}

metadata_folder = '/app/ws/metadata'
dags_folder = '/app/ws/dags'


def get_backup_folder():
    """ Каждый бэкап создается в отдельной директории на основе даты запуска.
    """
    backup_folder = '/app/ws/share/df/environment_backup'
    backup_folder = os.path.join(backup_folder, '{{ ts_nodash_with_tz }}') + os.sep
    return backup_folder


with DAG('df_backup_environment',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['system']) as dag:

    create_backup_folder = BashOperator(
        task_id='df_bash_backup_environment_create_backup_folder',
        bash_command=f'mkdir -p {get_backup_folder()}'
    )

    backup_metadata = BashOperator(
        task_id='df_bash_backup_environment_metadata',
        bash_command=f'cp -R {metadata_folder} {get_backup_folder()}'
    )

    backup_dags = BashOperator(
        task_id='df_bash_backup_environment_dags',
        bash_command=f'cp -R {dags_folder} {get_backup_folder()}'
    )

    close_dag = DummyOperator(
        task_id='df_dag_closing',
        dag=dag
    )

    create_backup_folder >> [backup_metadata, backup_dags] >> close_dag
