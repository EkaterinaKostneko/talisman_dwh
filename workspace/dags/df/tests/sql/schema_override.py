'''
Created on 21 Oct 2019

@author: pymancer

Тест создания системной таблицы в не основной для создающего пользователя схеме.
'''
from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner':            'admin',
    'description':      'Test Schema Override',
    'depend_on_past':   False,
    'start_date':       datetime(2019, 7, 29),
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          0
}

with DAG('df_test_schema_override',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['test']) as dag:
    environment = {'AF_EXECUTION_DATE': '{{ ds }}',
                   'AF_TASK_ID': '{{ task.task_id }}',
                   'AF_TASK_OWNER': '{{ task.owner }}',
                   'AF_RUN_ID': '{{ run_id }}',
                   'AF_DWH_DB_CONNECTION': serialize_connection('testmssql'),
                   'AF_DWH_DB_SCHEMA': 'aa_dwh.tuser2'
                   }

    ddl = DockerOperator(
        task_id='df_test_docker_schema_override',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment=environment,
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app/ws/source/df/sql/ddl',
        command='alembic upgrade head',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m'
    )
