from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner': 'airflow',
    'description': 'Basic Pivot Excel Extractor',
    'depend_on_past': False,
    'start_date': datetime(2019, 6, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'df_example_extract_excel_pivot'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }

with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['example'],
        doc_md='''### Пример DAG'а экстракта из кросс-таблицы.
Используются аннотации:
  - `isRow` - для указания шапки в качестве источника данных строки
  - `isPivot` - для указания загрузки данных кросс-таблицы
'''
) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/xlsx/excel_source_pivot.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/xlsx/excel_raw_pivot.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )
