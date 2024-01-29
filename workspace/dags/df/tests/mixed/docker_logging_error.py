from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.operators.bash import BashOperator
from operators.extended_docker_operator import ExtendedDockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner': 'tuser',
    'description': 'Docker Logging UnicodeDecodeError & Lost Container',
    'depend_on_past': False,
    'start_date': datetime(2019, 6, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'df_test_docker_logging_error'
task_id = f'{dag_id}.logging_error'
finish_task_id = f'{dag_id}.finish_task'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['test']) as dag:
    t1 = ExtendedDockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,  # При deferred_auto_remove=True принудительно устанавливается в False
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/tests/docker_logging_error.sql',
                        'AF_LOGLEVEL': 'debug'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        email_on_failure=True,
        email=['mikhaylov@polymedia.ru'],
        xcom_all=True
    )
    t2 = BashOperator(
        task_id=finish_task_id,
        bash_command='echo "Check previous step result: $env1"',
        env={'env1': f'QWERTY'}
    )
    t1 >> t2