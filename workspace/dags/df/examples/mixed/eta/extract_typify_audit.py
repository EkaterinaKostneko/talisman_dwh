from os import getenv
from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'ETA - Extract Typify Audit',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 8, 2),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql'),
               'AF_DWH_DB_SCHEMA': getenv('AF_DWH_DB_SCHEMA'),
               'AF_DWH_DB_SCHEMA_2': getenv('AF_DWH_DB_SCHEMA_2')
               }

with DAG('df_example_extract_typify_audit',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    prepare_entities = DockerOperator(
        task_id=f'df_example_docker_eta_prepare_entities',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_METADATA_PATH': f'/app/ws/metadata/df/models/examples/mixed/eta',
                        'AF_METADATA_RE': 'typify|country|product|client|transaction'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_create_entity.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    extract = DockerOperator(
        task_id='df_example_docker_eta_extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/mixed/eta/source.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/mixed/eta/raw.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m'
    )

    typify = DockerOperator(
        task_id='df_example_docker_eta_typify',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_JOB_ID': '{{ ti.job_id }}',
                        'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/typify_audit.sql',
                        'AF_PRODUCER': '/app/ws/metadata/df/models/examples/mixed/eta/raw.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/mixed/eta/typify.json',
                        'AF_DWH_DB_SCHEMA_SYS': environment['AF_DWH_DB_SCHEMA'],
                        'AF_DWH_DB_SCHEMA_DWH': environment['AF_DWH_DB_SCHEMA']}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m'
    )

    normalize = DockerOperator(
        task_id='df_example_docker_eta_normalize',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_JOB_ID': '{{ ti.job_id }}',
                        'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/eta/normalize_transaction.sql',
                        'AF_PRODUCER': '/app/ws/metadata/df/models/examples/mixed/eta/typify.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/mixed/eta/normalize/transaction.json',
                        'AF_DWH_DB_SCHEMA_2': environment['AF_DWH_DB_SCHEMA_2']
                        }
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m'
    )

    [extract, prepare_entities] >> typify >> normalize
