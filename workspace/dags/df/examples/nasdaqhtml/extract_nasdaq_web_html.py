from datetime import datetime

from airflow.models import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner': 'airflow',
    'description': 'nasdaqhtml Extractor',
    'depend_on_past': False,
    'start_date': datetime(2020, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }

dag_id = 'df_example_extract_nasdaq_web_html'

with DAG(
    dag_id,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example'],
    params={
       'dateBegin': '2021-11-25',
       'dateEnd': '2021-11-25'
    },
    doc_md='''### Пример DAG'а получения данных NASDAQ с общедоступного ресурса.
- все параметры `dag_run.conf` должны быть заданы в `params` и со значениями по умолчанию'''
) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION_PRODUCER': serialize_connection('http_nasdaqhtml_mocker_local'),
                        'AF_PRODUCER': '/app/ws/metadata/df/models/examples/nasdaqhtml/source_nasdaq_web_html.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/nasdaqhtml/raw_nasdaq_web_html.json',
                        'AF_DATE_BEGIN_VARIABLE': '{{ dag_run.conf.get("dateBegin", "") if dag_run.conf else params.get("dateBegin", "") }}',
                        'AF_DATE_END_VARIABLE': '{{ dag_run.conf.get("dateEnd", "") if dag_run.conf else params.get("dateEnd", "") }}',
                        'AF_PURGE_EXISTING_DATA': 'true'
                        }
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )
