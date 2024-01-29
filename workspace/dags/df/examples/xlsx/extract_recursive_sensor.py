'''
Created on 23 Oct 2019

@author: pymancer
'''
from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport
from sensors.local_file_sensor import LocalFileSensor # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Delayed Sensor',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 10, 23),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_extract_recursive_sensor'
upload_directory = '/app/ws/share/'
sensor_task_id = f'{dag_id}.sense'
extract_task_id = f'{dag_id}.extract'

now = datetime.utcnow().isoformat()

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}'
               }

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    sensor = LocalFileSensor(
        task_id=sensor_task_id,
        path=upload_directory,
        pattern='^[a-z]{4}_simple\.xlsx$',
        poke_interval=5,
        timeout=300,
        depth=0,
        do_xcom_push=True,
        lazy=True
    )

    extract = DockerOperator(
        task_id=extract_task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION': serialize_connection('testmssql'),
                        'AF_PRODUCER': '/app/ws/metadata/df/models/examples/xlsx/excel_source_recursive_sensor.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/xlsx/excel_raw_const.json',
                        'AF_FILEPATH': f'{{{{ ti.xcom_pull(task_ids="{sensor_task_id}") }}}}',
                        'AF_LOGLEVEL': 'info'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    sensor >> extract
