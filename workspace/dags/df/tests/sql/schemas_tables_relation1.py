from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner': 'airflow',
    'description': 'Creating table relationships from different schemas',
    'depend_on_past': False,
    'start_date': datetime(2019, 6, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }

with DAG('df_test_schemas_tables_relation1', default_args=default_args, schedule_interval=None,
         catchup=False, tags=['test']) as dag:
    prepare_entities = DockerOperator(
        task_id=f'df_test_docker_schemas_tables_relation1',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_METADATA_PATH': f'/app/ws/metadata/df/models/tests/sql/test_98/1/test_98_a.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_create_entity.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )
