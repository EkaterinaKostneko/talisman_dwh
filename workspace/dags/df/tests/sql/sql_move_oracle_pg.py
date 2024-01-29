from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'SQL Move Oracle PG',
    'depend_on_past'        : False,
    'start_date'            : datetime(2021, 1, 25),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}'
               }

dag_id = 'df_test_sql_move_oracle_pg'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['test']) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.move',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION_PRODUCER': serialize_connection('oracle_341_lab'),
                        'AF_DWH_DB_CONNECTION_CONSUMER': serialize_connection('testpostgresql'),
                        'AF_PRODUCER_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/tests/sql_move_producer_oracle_pg.sql',
                        'AF_CONSUMER_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/tests/sql_move_consumer_pg.sql',
                        'AF_BATCH_SIZE': 500,
                        'AF_LOGLEVEL': 'debug'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run_sql_move.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mem_limit='4g'
    )
