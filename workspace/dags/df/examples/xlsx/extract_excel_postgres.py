'''
Created on 20 Jun 2019

@author: pymancer
'''
from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Postgres Excel Extractor',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 20),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_extract_excel_postgres'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}'
               }

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql'),
                        'AF_PRODUCER': '/app/ws/metadata/df/models/examples/xlsx/excel_source_postgres.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/xlsx/excel_raw_postgres.json',
                        'AF_DATE_BEGIN_VARIABLE': '20190101',
                        'AF_DATE_END_VARIABLE': '20191231',
                        'AF_UNIT_ID_VARIABLE': 'postgres'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )
