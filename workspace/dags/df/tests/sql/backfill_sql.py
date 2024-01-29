from datetime import datetime, timedelta

from airflow.models import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Backfill',
    'depend_on_past'        : False,
    'start_date'            : datetime(2022, 5, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql')
               }
dag_id = 'df_test_backfill'

with DAG(dag_id,
         default_args=default_args,
         #schedule_interval=None,
         schedule_interval=timedelta(days=1),
         catchup=True,
         max_active_runs=2,
         tags=['test']) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.execute',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/tests/backfill.sql'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )
