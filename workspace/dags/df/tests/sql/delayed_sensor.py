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

dag_id = 'df_test_delayed_sensor'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }

poke_interval = 10
trigger_delay = 5
now = datetime.utcnow().isoformat()

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['test']) as dag:
    sensor_task = LocalFileSensor(
        task_id=f'{dag_id}.df_test_filesensor_delayed_sensor',
        path='.',
        pattern='.+',
        poke_interval=poke_interval,
        trigger_delay=trigger_delay,
        timeout=300)

    trigger_task = DockerOperator(
        task_id='df_test_docker_delayed_sensor',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/tests/delayed_sensor.sql',
                        'AF_EXECUTION_TIME': '{{ ts }}',
                        'AF_NOW': now,
                        'AF_SINGLETRAN': True}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_statement.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    sensor_task >> trigger_task
