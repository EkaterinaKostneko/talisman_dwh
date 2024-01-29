"""
Created on Aug 7, 2019

@author: pymancer
"""
from os import getenv
from datetime import datetime
from docker.types import Mount
from airflow.models import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner':            'admin',
    'description':      'Populate Data DB Tables From JSON',
    'depend_on_past':   False,
    'start_date':       datetime(2019, 8, 7),
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          0
}

dag_id = 'df_datadb_populate'

with DAG(dag_id,
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['system']) as dag:
    environment = {'AF_EXECUTION_DATE': '{{ ds }}',
                   'AF_DAG_ID': dag_id,  # обязательно наличие в env, для загрузки метаинформации о выполняемых задачах
                   'AF_TASK_ID': '{{ task.task_id }}',
                   'AF_TASK_OWNER': '{{ task.owner }}',
                   'AF_RUN_ID': '{{ run_id }}',
                   'AF_JOB_ID': '{{ ti.job_id }}',
                   'AF_DWH_DB_CONNECTION': serialize_connection('df')
                   }

    t1 = DockerOperator(
        task_id=f'{dag_id}.remove_unused_sources',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/python/scripts/remove_unused_sources.pyj',
                        'AF_FILES': '/app/ws/share/df/unused_sources.txt'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )

    t2 = DockerOperator(
        task_id=f'{dag_id}.merge',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION': serialize_connection('df'),
                        'AF_DWH_DB_SCHEMA': getenv('DATAFLOW_SCHEMA'),
                        'AF_JSON_PATH': '/app/ws/source/df/sql/data',
                        'AF_JSON_RE': r'.+\.json$'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/merge_data_json.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m',
        mount_tmp_dir=False
    )

    t1 >> t2
