from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Complex Hidden Excel Extractor',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 7, 24),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

with DAG('df_example_extract_excel_complex_hidden',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    t1 = DockerOperator(
        task_id='df_example_docker_extract_excel_complex_hidden',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': '{{ ds }}',
            'AF_TASK_OWNER': '{{ task.owner }}',
            'AF_TASK_ID': '{{ task.task_id }}',
            'AF_RUN_ID': '{{ run_id }}',
            'AF_DWH_DB_CONNECTION': serialize_connection('testmssql'),
            'AF_PRODUCER': '/app/ws/metadata/df/models/examples/xlsx/excel_source_complex_hidden.json',
            'AF_CONSUMER': '/app/ws/metadata/df/models/examples/xlsx/excel_raw_complex_hidden.json'
        },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m'
    )
