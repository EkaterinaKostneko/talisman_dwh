from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Excel 2003 XML Extractor',
    'depend_on_past'        : False,
    'start_date'            : datetime(2020, 12, 17),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_extract_excel2003xml'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    extract_task = DockerOperator(
        task_id=f'{dag_id}.extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/excel2003xml/source_excel2003xml.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/excel2003xml/raw_excel2003xml.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )
