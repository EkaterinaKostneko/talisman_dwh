'''
Created on 23 Jul 2019

@author: pymancer
'''
from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Triggerable Excel Extractor Simple 1',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 7, 23),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

with DAG('df_test_extract_excel_triggerable_simple_1', default_args=default_args, schedule_interval=None,
         catchup=False, tags=['test']) as dag:
    triggered_task = DockerOperator(
        task_id='df_test_docker_extract_excel_triggerable_simple_1',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': '{{ ds }}',
            'AF_TASK_OWNER': '{{ task.owner }}',
            'AF_TASK_ID': '{{ task.task_id }}',
            'AF_RUN_ID': '{{ run_id }}',
            'AF_PRODUCER': '/app/ws/metadata/df/models/tests/xlsx/excel_source_triggerable_simple_1.json',
            'AF_CONSUMER': '/app/ws/metadata/df/models/tests/xlsx/excel_raw_triggerable.json',
            'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql'),
            'AF_DATE_BEGIN_VARIABLE': '20190101',
            'AF_DATE_END_VARIABLE': '20191231',
            'AF_UNIT_ID_VARIABLE': 'triggerable'
        },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )
