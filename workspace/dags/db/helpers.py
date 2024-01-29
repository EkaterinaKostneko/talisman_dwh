from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowException
from docker.types import Mount
from plugins.common.helpers import serialize_connection  # @UnresolvedImport
from operators.visiology_operator import VisiologyAPIOperator

# конфигурационный файл проекта
import db.config as config

# переменные окружения передаются в Docker-контейнер
environment = {
    'AF_EXECUTION_DATE': '{{ ds }}',
    'AF_START_DATE': '{{ dag_run.start_date }}',
    'AF_DAG_ID': '{{ dag.dag_id }}',
    'AF_TASK_ID': '{{ task.task_id }}',
    'AF_TASK_OWNER': '{{ task.owner }}',
    'AF_RUN_ID': '{{ run_id }}',
    'AF_LOGLEVEL': config.log_level,

    # sys.load.id текущего дага, скрипт get_load_id должен запускаться первым в даге
    'AF_LOAD_ID': f'{{{{ ti.xcom_pull(task_ids="{"get_load_params"}")'
                  f'if ti.xcom_pull(task_ids="{"get_load_params"}") else "" }}}}',

    # sys.load.id дага, запустившего текущий даг
    'AF_PARENT_LOAD_ID': f'{{{{ dag_run.conf["parent_load_id"] if dag_run.conf.get("parent_load_id") else "" }}}}',

    # параметры запуска, переданные из интерфейса при запуске(значения атрибутов JSON по умолчанию в даге должны быть равны None или "")
    'AF_PARAMS': f'{{{{ params|tojson }}}}',

    # параметры запуска, переданные из интерфейса запуска дага, запустившего текущий даг
    # 'AF_PARENT_PARAMS': f'{{{{ dag_run.conf["params"] if dag_run.conf.get("params") else "" }}}}',

    # даты периода для условий выборки, генерируемые скриптом get_load_inc
    'AF_INC_BEGIN': f'{{{{ ti.xcom_pull(task_ids="{"get_load_inc"}").split(";")[0]'
                    f'if ti.xcom_pull(task_ids="{"get_load_inc"}") else "" }}}}',
    'AF_INC_END': f'{{{{ ti.xcom_pull(task_ids="{"get_load_inc"}").split(";")[1]'
                  f'if ti.xcom_pull(task_ids="{"get_load_inc"}") else "" }}}}',

    # 'AF_INC_BEGIN': get_inc.dt_begin,
    # 'AF_INC_END': get_inc.dt_end,

    # даты периода для условий выборки, переданные из дага, запустившего текущий даг
    # 'AF_PARENT_INC_BEGIN': f'{{{{ dag_run.conf["parent_inc_begin"] if dag_run.conf.get("parent_inc_begin") else "" }}}}',
    # 'AF_PARENT_INC_END': f'{{{{ dag_run.conf["parent_inc_end"] if dag_run.conf.get("parent_inc_end") else "" }}}}',

    'AF_MIN_LOAD_DATE': config.min_load_date,
    'AF_MAX_LOAD_DATE': config.max_load_date,
}


sql_path = f'/app/ws/source/{config.project_dir}/sql'
python_path = f'/app/ws/source/{config.project_dir}/python'
metadata_path = f'/app/ws/metadata/{config.project_dir}'
share_path = f'/app/ws/share/{config.project_dir}/to_process' 
cache_path = f'/app/ws/cache/{config.project_dir}'


def run_sql(script, *, params={}, use_short=False, task_id=None, trigger_rule='all_success'):
    """ Возвращает таск запуска sql-скрипта
            param: script - путь к sql-скрипту относительно каталога sql_path
            param: params - словарь переменных, добавляемых в переменные окружения и доступных шаблонизатору для вставки в sql-скрипт
            param: use_short - признак использования для генерации имени таска только имени скрипта без пути, для скриптов с совпадающими именами из разных папок выставлять false
            param: task_id - наименование таска, если не задано, то генерируется на основе script

    """

    if not task_id:
        if use_short:
            task_id = script.split('/')[-1].rsplit('.', 1)[0]
        else:
            task_id = script.replace('/', '_').rsplit('.', 1)[0]
        task_id = f'sql_{task_id}'
    else:
        task_id = str(task_id or '')

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': f'{sql_path}/{script}',
                        'AF_DWH_DB_CONNECTION': serialize_connection(config.dwh_db_conn)}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        trigger_rule=trigger_rule
    )



def extract_excel(subdir, schema, entity, from_dir, params={}, task_id=None, n_retries=0):
    """ Возвращает таск извлечения данных из файлов Excel (включая csv)
            param: subdir - относительный путь к каталогу метаданных
            param: schema - схема целевой таблицы
            param: entity - целевая сущность, используется для определения имен файлов метаданных
            param: from_dir - каталог с файлами для извлечения данных
            param: task_id - наименование таска, если не задано, то генерируется на основе entity
    """
    if not task_id:
        task_id = f'extract_excel_{entity}'
    else:
        task_id = str(task_id or '')

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION': serialize_connection(config.dwh_db_conn),
                        'AF_PRODUCER': f'{metadata_path}/{subdir}/{entity}_producer.json', 
                        'AF_CONSUMER': f'{metadata_path}/{subdir}/{entity}_consumer.json',
                        'AF_FILEPATH': f'{from_dir}',
                        'AF_DWH_DB_SCHEMA': schema},
                        **params
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        retries=n_retries
    )


def run_python(script, *, params={}, task_id=None, use_short=False, trigger_rule='all_success'):
    if not task_id:
        if use_short:
            task_id = script.split('/')[-1].rsplit('.', 1)[0]
        else:
            task_id = script.replace('/', '_').rsplit('.', 1)[0]
        task_id = f'py_{task_id}'
    else:
        task_id = str(task_id or '')

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': f'{python_path}/{script}',
                        'AF_DWH_DB_CONNECTION': serialize_connection(config.dwh_db_conn)
                        },
                     **params,
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        trigger_rule=trigger_rule
    )



def step(task_id):
    return DummyOperator(
        task_id=task_id
    )


def get_load_params(source_system=None, stage=None):
    return run_python('get_load_params.py',
                      params={'AF_SOURCE_SYSTEM': source_system, 'AF_STAGE' : stage},
                      task_id='get_load_params')


def get_load_id(source_system=None, stage=None):
    return run_python(
        'get_load_id.py',
        params={
            'AF_SOURCE_SYSTEM': source_system,
            'AF_STAGE': stage,
            'AF_LOGLEVEL': 'info'
        },
        task_id='get_load_id'
    )


def finish_load(trigger_rule='all_success'):
    return run_python('finish_load.py', task_id='finish_load', trigger_rule=trigger_rule)


def push_loadplan_id_by_name(__, plans, plan_name):
    """ Возвращает id плана по его имени, что автоматически пушит его как XCOM. """
    plan_id = None
    plan_name_lower = plan_name.lower()

    for plan in plans:
        if plan['name'].lower() == plan_name_lower:
            plan_id = plan['id']
            break

    if not plan_id:
        raise AirflowException(f'Plan <{plan_name}> not found.')

    return plan_id


def get_plan(plan_name, task_id=None):
    """
    Получает ID плана загрузки
    :param task_id: идентификатор Airflow таска
    :param plan_name: наименование плана загрузки
    :return:
    """
    if not task_id:
        task_id = f'get_plan_{plan_name.replace(" ", "")}'
    return VisiologyAPIOperator(
        task_id=task_id,
        resource=f'{config.vqadmin_api}/loadplans',
        visiology_conn_id=config.visiology_conn,
        response_handler=push_loadplan_id_by_name,
        handler_args=(plan_name, )
    )


def start_plan(plan_name=None, prev_task_id=None, task_id=None):
    """
    Запускает план загрузки
    :param task_id: идентификатор Airflow таска
    :param prev_task_id: идентификатор таска, где получаем ID плана
    :return:
    """
    if not task_id:
        task_id = f'start_plan_{plan_name.replace(" ", "")}'
    if not prev_task_id:
        prev_task_id = f'get_plan_{plan_name.replace(" ", "")}'
    return VisiologyAPIOperator(
        task_id=task_id,
        resource=(f'{config.vqadmin_api}/loadplans/{{{{ ti.xcom_pull(task_ids="{prev_task_id}") }}}}/start'),
        method='post',
        visiology_conn_id=config.visiology_conn
    )

def extract_sql(source_conn, subdir, entity, params={}, task_id=None):
    """ Возвращает таск извлечения данных из БД источника в БД хранилища
            param: source_conn - соединение с БД источника
            param: subdir - относительный путь каталогу скриптов
            param: entity - наименование сущности, используется для определения имен sql-скриптов извлечения и вставки данных, передается в sql-скрипт извлечения через переменную AF_ENTITY
            param: params - словарь переменных, добавляемых в переменные окружения и доступных шаблонизатору для вставки в sql-скрипт
            param: task_id - наименование таска, если не задано, то генерируется на основе entity
    """
    if task_id is None:
        task_id = f'extract_{entity}'
    else:
        task_id = str(task_id or '')

    return DockerOperator(
        task_id=task_id,
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_DWH_DB_CONNECTION_PRODUCER': serialize_connection(source_conn),
                        'AF_DWH_DB_CONNECTION_CONSUMER': serialize_connection(config.dwh_db_conn),
                        'AF_PRODUCER_SCRIPT_PATH': f'{sql_path}/{subdir}/{entity}_producer.sql',
                        'AF_CONSUMER_SCRIPT_PATH': f'{sql_path}/{subdir}/{entity}_consumer.sql',
                        },
                     **params
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/python/run_sql_move.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mem_limit='4g'
    )

def get_inc(task_id=None):
    if not task_id:
        task_id = f'get_inc'
    return run_python(
        'get_inc.py',
        params={
            'AF_LOGLEVEL': 'info'
        },
        task_id=task_id
    )


def set_inc(entities_inc, task_id=None, trigger_rule='all_success'):
    if not task_id:
        task_id = f'set_inc'
    return run_python(
        'set_inc.py',
        params={
            'AF_ENTITIES_INC': json.dumps(entities_inc),
            'AF_LOGLEVEL': 'info'
        },
        task_id=task_id,
        trigger_rule=trigger_rule
    )