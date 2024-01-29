from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Dwash Generic',
    'depend_on_past'        : False,
    'start_date'            : datetime(2020, 12, 15),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}'
               }

dag_id = 'df_example_loader_dwash_generic'
loader_task_id = f'{dag_id}.loader'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    loader = DockerOperator(
        task_id=loader_task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     # приоритет токена подключения к облаку: password, Extra.token, AF_LOADER_TOKEN
                     **{'AF_PROVIDER_CONNECTION': serialize_connection('dwash_local'),
                        # все последующие переменные переопределяют соотвествующие Connection.Extra (без 'AF_LOADER_')
                        'AF_LOADER_RUN': 'ONCE',  # 'ONCE' - однократно, 'WAIT' - до результатов, 'REPEAT' - повторять
                        'AF_LOADER_CONTAINER': '',  # директория бакета, в которой лежат искомые файлы
                        'AF_LOADER_INTERVAL': 300,  # интервал в секундах между проверками наличия файлов по маске
                        'AF_LOADER_TIMEOUT': 86400,  # максимальное время ожидания появления файла (в секундах)
                        'AF_LOADER_PROVIDER': 'DWASH',  # код провайдера (известного лоадера)
                        'AF_LOADER_PATTERN': '^.+\.xlsx$',  # маска поиска файла по имени (regular expression match)
                        'AF_LOADER_MODE': 'COPY',  # 'COPY' - загружать файл, 'CUT' - загружать и удалять файл из облака
                        'AF_LOADER_STORAGE': str(loader_task_id),
                        'AF_LOADER_DESCENT': 0}  # сохранять структуру, 1 (по умолчанию) - только файл, 0 - все уровни
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run_loader.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mount_tmp_dir=False
    )

    loader
