from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Create Sequence Test',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_create_sequence'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               # custom
               'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/sequence_table_drop.sql',
               'AF_METADATA_PATH': '/app/ws/metadata/df/models/examples/sql/create_sequence.json'
               }

environment_mssql = {
    'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
}

environment_postgres = {
    'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql')
}

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.df_example_create_sequence_mssql',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **environment_mssql},
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_create_entity.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t2 = DockerOperator(
        task_id=f'{dag_id}.df_example_create_sequence_postgres',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **environment_postgres},
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_create_entity.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t3 = DockerOperator(
        task_id=f'{dag_id}.df_example_drop_sequence_mssql',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **environment_mssql},
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_statement.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t4 = DockerOperator(
        task_id=f'{dag_id}.df_example_drop_sequence_postgres',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **environment_postgres},
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_statement.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t1 >> t3
    t2 >> t4
