from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner': 'airflow',
    'description': 'Dwash Generic',
    'depend_on_past': False,
    'start_date': datetime(2020, 12, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_PREVIOUS_EXECUTION_DATE': '{{ prev_ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}'
               }

dag_id = 'df_example_loader_ftp_generic'
loader_task_id = f'{dag_id}.loader'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    loader = DockerOperator(
        task_id=loader_task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     # приоритет токена подключения к облаку: password, Extra.token, AF_LOADER_TOKEN
                     **{'AF_PROVIDER_CONNECTION': serialize_connection('ftp_local'),
                        # все последующие переменные переопределяют соотвествующие Connection.Extra (без 'AF_LOADER_')
                        'AF_LOADER_PROVIDER': 'FTP',  # код провайдера (известного лоадера: FTP, FTPS )
                        'AF_LOADER_INTERVAL': 300,  # интервал в секундах между проверками наличия файлов по маске
                        'AF_LOADER_TIMEOUT': 86400,  # максимальное время ожидания появления файла (в секундах)

                        'AF_LOADER_RUN': 'ONCE',  # 'ONCE' - однократно, 'WAIT' - до результатов, 'REPEAT' - повторять

                        # Режимы загрузки:
                        #   'EMPTY' - очистить локальную папку перед загрузкой,
                        #   'COPY' - загружать файл,
                        #   'CUT' - загружать и удалять файл из удаленного хоста,
                        #   'MULTIPATTERN' - передача списка наименований (масок) файлов, поиск которых осуществляется относительно CONTAINER,
                        #   'MULTICONTAINER' - передача списка контейнеров (рабочих директорий), относительно которых выполняется поиск по PATTERN
                        #   'UNPACK' - распаковать загруженные архивы
                        #   'DELETE' - удалить загруженные файлы
                        'AF_LOADER_MODE': 'COPY',

                        # маска поиска файла по имени (regular expression match):
                        # '.*', '^.+\.xlsx$',
                        # для режима 'MULTIPATTERN' - b'["file.a", "file.b"]'.decode('utf-8'), f'{{{{ ti.xcom_pull(task_ids="{push_task_id}") }}}}'
                        'AF_LOADER_PATTERN': '.*',

                        # директория на FTP относительно рабочей, в которой лежат искомые файлы,
                        # для режима 'MULTICONTAINER' - b'["/dir_1", "/dir_2"]'.decode('utf-8'), f'{{{{ ti.xcom_pull(task_ids="{push_task_id}") }}}}'
                        'AF_LOADER_CONTAINER': '',

                        # максимальный уровень дерева каталогов, в которых будет производиться поиск
                        # (None, 0 - нет ограничений, 1 - корневой каталог)
                        'AF_LOADER_LEVEL': None,
                        # минимальный уровень дерева каталогов для скачивания файлов (None, 0, 1 - корневой каталог)
                        'AF_LOADER_MIN_LEVEL': None,
                        # файл с функцией фильтрации, например - '/app/ws/source/df/python/scripts/examples/filter_files.pyj::filter_by_date'
                        # 'AF_LOADER_FILTER': '/app/ws/source/df/python/scripts/examples/filter_files.pyj::filter_by_date',
                        'AF_LOADER_STORAGE': str(loader_task_id),
                        'AF_LOADER_OVERWRITE': True}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run_loader.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host'
    )
