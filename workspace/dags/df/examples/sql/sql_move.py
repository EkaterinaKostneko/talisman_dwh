from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'SQL Move',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

# общий префикс переменных окружения для упрощения переключения БД
src_prefix = 'DWHPG_DB_'
dest_prefix = 'DWH_DB_'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION_PRODUCER': serialize_connection('testpostgresql'),
               'AF_DWH_DB_CONNECTION_CONSUMER': serialize_connection('testmssql'),
               'AF_LOGLEVEL': 'debug'
               }

dag_id = 'df_example_sql_move'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.postgresql_to_mssql',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{
                         'AF_PRODUCER_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/sql_move_producer.sql',
                         'AF_CONSUMER_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/sql_move_consumer.sql',
                         'AF_BATCH_SIZE': 500}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run_sql_move.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mem_limit='4g'
    )
