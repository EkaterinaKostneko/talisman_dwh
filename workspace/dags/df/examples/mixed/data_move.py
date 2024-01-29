from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Data Move',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_SRC_CONNECTION': serialize_connection('testmssql'),
               'AF_DEST_CONNECTION': serialize_connection('testpostgresql'),
               'AF_INSERT_BATCH': 1000
               }

with DAG('df_example_data_move',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    t1 = DockerOperator(
        task_id='df_example_docker_data_move',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/python/scripts/examples/move_data.pyj'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )
