from pathlib import PosixPath
from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport
from operators.netdb_operator import NetDBAPIOperator # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Web NetDB Extractor',
    'depend_on_past'        : False,
    'start_date'            : datetime(2020, 3, 25),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_extract_netdb_web'
download_task_id = f'{dag_id}.df_example_docker_extract_netdb_web_download'
# идентификатор объекта Airflow Connection хранящий подключение к сервису NetDB
netdb_conn_id = 'netdb_default'
years = [
    2019
]
units = [
    1827006
]

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }


def download_and_push(__, xml, year, unit):
    """ Сохраняет загруженный XML в файл и возвращает путь, что автоматически пушит его как XCOM.
        TODO: возможно стоит разработать отдельный оператор загрузки файлов относительно заданного каталога
    """
    # директория для сохранения загруженных данных и справочников
    download_dir = '/app/ws/share/df/examples/netdb/download'
    download_dir = PosixPath(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    path = download_dir.joinpath(f'test_{year}_{unit}.xml')
    path.write_text(xml)

    return path


etl = list()

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    for year in years:
        for unit in units:
            download_task_id_unit = f'{download_task_id}_{year}_{unit}'
            extract_task_id_unit = f'{dag_id}.df_example_docker_extract_netdb_web_extract_{year}_{unit}'

            download = NetDBAPIOperator(
                task_id=download_task_id_unit,
                resource=f'330372/?param={year}&param={unit}&headers=true',
                netdb_conn_id=netdb_conn_id,
                response_handler=download_and_push,
                handler_args=(year, unit)
            )

            extract = DockerOperator(
                task_id=extract_task_id_unit,
                image='df_sql_excel:latest',
                api_version='auto',
                auto_remove=True,
                environment={**environment,
                             **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/netdb/source_web.json',
                                'AF_CONSUMER': '/app/ws/metadata/df/models/examples/netdb/raw_web.json',
                                'AF_SOURCE_PATH': f'{{{{ ti.xcom_pull(task_ids="{download_task_id_unit}") }}}}'}
                             },
                mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
                working_dir='/app',
                command='python /app/ws/source/df/excel/run.py',
                docker_url='unix://var/run/docker.sock',
                network_mode='bridge'
            )

            extract.set_upstream(download)
            etl.append([download, extract])

    etl
