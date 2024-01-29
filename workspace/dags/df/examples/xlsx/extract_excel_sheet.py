from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Sheet Excel Extractor',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 2),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

with DAG('df_example_extract_excel_sheet',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    t1 = DockerOperator(
        task_id='df_example_docker_extract_excel_sheet',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': '{{ ds }}',
            'AF_TASK_OWNER': '{{ task.owner }}',
            'AF_TASK_ID': '{{ task.task_id }}',
            'AF_RUN_ID': '{{ run_id }}',
            'AF_PRODUCER': '/app/ws/metadata/df/models/examples/xlsx/excel_source_sheet.json',
            'AF_CONSUMER': '/app/ws/metadata/df/models/examples/xlsx/excel_raw_sheet.json',
            'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
        },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )
