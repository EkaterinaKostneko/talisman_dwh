"""
Пример адаптера, загружающего данные переданных текстовых файлов в базу.
- `AF_PRODUCER` - описание источника (файла) для загрузки
- `AF_CONSUMER` - описание таблицы для загрузки
- `AF_DWH_DB_CONNECTION` - подключение к базе-хранилища
"""
from datetime import datetime
from airflow.models import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

DAG_ID = 'df_example_extract_content_text'

default_args = {
    'owner': 'airflow',
    'description': 'Text File Content Extractor',
    'depend_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_DAG_ID': DAG_ID,  # обязательно наличие в env, для загрузки метаинформации о выполняемых задачах
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_JOB_ID': '{{ ti.job_id }}'
               }

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example']
)
t1 = DockerOperator(
    dag=dag,
    task_id=f'{DAG_ID}.extract',
    image='df_operator:latest',
    api_version='auto',
    auto_remove=True,
    environment={**environment,
                 **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/content/source_text.json',
                    'AF_CONSUMER': '/app/ws/metadata/df/models/examples/content/raw_text.json',
                    'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql')
                    }
                 },
    mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
    working_dir='/app',
    command='python /app/ws/source/df/excel/run.py',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge'
)

dag.doc_md = __doc__
