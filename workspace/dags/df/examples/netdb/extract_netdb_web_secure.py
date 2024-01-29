from json import dumps
from pathlib import PosixPath
from datetime import datetime
from airflow import DAG
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

dag_id = 'df_example_extract_netdb_web_secure'
download_task_id = f'{dag_id}.df_example_docker_extract_netdb_web_secure_download'
# идентификатор объекта Airflow Connection хранящий подключение к сервису NetDB
netdb_conn_id = 'netdb_secure'
dictionaries = [
    176131
]

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }


def download_and_push(__, data, dictionary):
    """ Сохраняет загруженный XML в файл и возвращает путь, что автоматически пушит его как XCOM.
        TODO: возможно стоит разработать отдельный оператор загрузки файлов относительно заданного каталога
    """
    # директория для сохранения загруженных данных и справочников
    download_dir = '/app/ws/share/df/netdb/download'
    download_dir = PosixPath(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    path = download_dir.joinpath(f'test_secure_{dictionary}.json')
    path.write_text(dumps(data))

    return path


downloads = list()

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    for dictionary in dictionaries:
        download_task_id_dictionary = f'{download_task_id}_{dictionary}'

        download = NetDBAPIOperator(
            task_id=download_task_id_dictionary,
            resource=f'backbone/set/{dictionary}',
            netdb_conn_id=netdb_conn_id,
            response_handler=download_and_push,
            handler_args=(dictionary,)
        )

        downloads.append([download])

    downloads
