from os import getenv
from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'To DC Dimension',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

# общий префикс переменных окружения для упрощения переключения БД
prefix = 'DWH_DB_'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}'
               }

dag_id = 'df_example_dc_dimelements_update'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.update',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION_PRODUCER': serialize_connection('testmssql'),
                        'AF_DWH_DB_CONNECTION_CONSUMER': serialize_connection('http_dc_mocker'),
                        'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/dc_dimelements_update.sql',
                        'AF_ENDPOINT': 'datacollection/api/dimensions/19/elements',
                        'AF_DWH_DB_SCHEMA_PRODUCER': getenv(f'{prefix}SCHEMA'),
                        'AF_OPERATION': 'update'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run_sql_to_viapi.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )
