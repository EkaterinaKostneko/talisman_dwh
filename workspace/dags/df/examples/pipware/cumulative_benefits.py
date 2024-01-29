from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import get_loadplan_id, get_plan_start_url, get_loadplans_url, serialize_connection
from operators.visiology_operator import VisiologyAPIOperator  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Cumulative Benefits',
    'depend_on_past'        : False,
    'start_date'            : datetime(2020, 6, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

# общий префикс переменных окружения для упрощения переключения БД
prefix = 'DWH_DB_'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}'
               }

dag_id = 'df_example_pipware_cumulative_benefits'
db_id = 'DB'
plan_name = 'cumulativeBenefits'
visiology_conn_id = 'visiology_ugmk'
get_plan_task_id = f'{dag_id}.get_plan'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    extract = DockerOperator(
        task_id=f'{dag_id}.extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION_PRODUCER': serialize_connection('pipware_ugmk'),
                        'AF_DWH_DB_CONNECTION': serialize_connection('dwh_ugmk'),
                        'AF_PRODUCER': '/app/ws/metadata/df/models/examples/pipware/source_cumulative_benefits.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/pipware/raw_cumulative_benefits.json',
                        'AF_DWH_DB_SCHEMA': 'ugmk'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )

    get_plan = VisiologyAPIOperator(
        task_id=get_plan_task_id,
        resource=get_loadplans_url(db_id),
        visiology_conn_id=visiology_conn_id,
        response_handler=get_loadplan_id,
        handler_args=(plan_name,)
    )

    start_plan = VisiologyAPIOperator(
        task_id=f'{dag_id}.start_plan',
        resource=get_plan_start_url(db_id, get_plan_task_id),
        method='post',
        visiology_conn_id=visiology_conn_id
    )

    extract >> get_plan >> start_plan
