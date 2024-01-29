"""
Created on 11 Jul 2019

@author: pymancer
"""
import os
import re
import shutil
import json
import tarfile
import zipfile

from box import Box
from typing import Union
from time import time
from urllib.parse import quote_plus
from pathlib import PosixPath, Path
from pathspec import PathSpec
from jinja2 import Template
from jsqlib.helpers.common import Tokenizer
from base64 import b64decode
from sqlalchemy.dialects import registry
from cryptography.fernet import Fernet
from collections import defaultdict

from df.common.constants import (SYSTEM_CONNECTION_ENV, SYSTEM_SCHEMA_ENV, DEFAULT_SCHEMA_ENV,  # @UnresolvedImport
                                 POSTGRES_PORT, MSSQL_PORT, MSSQL_OPTION_MAPPER)  # @UnresolvedImport
from df.common.schemas.model import Model  # @UnresolvedImport
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport

JSQL_TRANSLATOR = Box({
    "cast": {
        "true": "True",
        "false": "False",
        "null": "None"
    },
    "quote": {
        'double': {
            "left": '"',
            "right": '"',
            "delimiter": None
        },
        'single': {
            "left": "'",
            "right": "'",
            "delimiter": None
        }
    }
}, frozen_box=True)

# дополнительные диалекты для sqlalchemy
registry.register('qhb.psycopg2', 'common.dialects.qhb', 'QHBDialect')

log = Logger()


class Timer:
    def __init__(self):
        self.start = time()

    def __call__(self):
        return time() - self.start


def fix_postfix(postfix, upper=True):
    if not postfix:
        postfix = ''

    postfix = postfix.strip()

    if postfix:
        postfix = f"_{postfix.strip('_')}"

    return postfix.upper() if upper else postfix


def get_clean_schema(value, env_var=True, dotted=False):
    """ Возвращает схему без бд (при необходимости - как префикс к таблице) """
    schema = os.getenv(value, '') if env_var else value
    dot = '.' if dotted else ''
    schema = schema.split('.')[-1]
    return f'{schema}{dot}' if schema else ''


def get_conn_schema(postfix=None, decoded_connection=None):
    schema = ''
    # схема может быть задана в целом для подключения
    conn = decoded_connection or get_decoded_connection()

    extra = conn.get('extra', dict())
    if extra.get('schema'):
        schema_key = f'schema{fix_postfix(postfix)}'
        schema = get_clean_schema(extra.get(schema_key, extra.get('schema')), env_var=False)

    return schema


def get_db_schema(schema=None, postfix='', decoded_connection=None, dotted=False):
    """ Возвращает схему либо из виртуального окружения, убирая из нее имя базы, либо из Connection.
        Схема из переменной окружения с заданным постфиксом переопределяет дефолтную.
        Явно переданная схема переопределяет все остальные.
    """
    if not schema:
        # схема может быть задана отдельно для источника и получателя
        schema = get_clean_schema(f'{DEFAULT_SCHEMA_ENV}{fix_postfix(postfix)}')

    if not schema and (decoded_connection or os.getenv(f'AF_DWH_DB_CONNECTION{fix_postfix(postfix)}')):
        schema = get_conn_schema(postfix=postfix, decoded_connection=decoded_connection)

    if not schema:
        # может использоваться одна схема для всех используемых подключений (либо подключение только одно)
        schema = get_clean_schema(DEFAULT_SCHEMA_ENV)

    return f'{schema}.' if dotted and schema else schema


def get_df_connection():
    """ Возвращает системное подключение с кодом `df`, которое должно быть задано в подключениях. В переменные окружения
        докер-оператора добавляется автоматически, если не передано явно
    """
    return os.getenv(SYSTEM_CONNECTION_ENV)


def get_df_schema(dotted=False, connection=None):
    conn = get_decoded_connection(encoded=connection)
    schema = get_conn_schema(decoded_connection=conn) if conn and conn['id'] == 'df' else None
    schema = schema or os.getenv(SYSTEM_SCHEMA_ENV, os.getenv('DATAFLOW_SCHEMA', 'df'))
    return get_db_schema(schema=schema, dotted=dotted)


def get_fernet():
    fernet_path = PosixPath('/run/secrets/fernet')
    if fernet_path.is_file():
        # контейнер operator
        fernet = fernet_path.read_text()
    else:
        # контейнер airflow
        fernet = os.getenv('AIRFLOW__CORE__FERNET_KEY')

    return Fernet(fernet) if fernet else None


def get_oracle_driver():
    folder = os.getenv('LD_LIBRARY_PATH')
    try:
        __, major, minor = folder.split('_')
    except (AttributeError, ValueError):
        raise DFOperatorException('Не задана переменная окружения LD_LIBRARY_PATH. Подключение к Oracle невозможно.')

    return f'{folder}/libsqora.so.{major}.{minor}'


def get_decoded_connection(encoded=None, postfix=''):
    """ Возвращает словарь, заполненный параметрами подключения Airflow Connection, если они есть.
        Ключи Connection: {'type', 'host', 'schema', 'login', 'pass', 'port', 'extra'}, где schema ~ db
    """
    fernet = None
    decoded = defaultdict(lambda: None)
    encoded = encoded or os.getenv(f'AF_DWH_DB_CONNECTION{fix_postfix(postfix)}')

    if encoded:
        try:
            decoded = json.loads(b64decode(bytes(encoded, encoding='utf-8')).decode('utf-8'))
        except Exception as e:
            log.warning(f'Ошибка декодирования параметров подключения: {e}')
        else:
            conn_id = decoded.get('id')
            is_exists = decoded.pop('is_exists')

            if not is_exists:
                raise DFOperatorException(f'Объект Airflow Connection <{conn_id}> не найден')

            fernet = get_fernet()

            if decoded.pop('is_encrypted', False) and fernet and decoded.get('pass'):
                try:
                    decoded['pass'] = fernet.decrypt(bytes(decoded['pass'], 'utf-8')).decode()
                except Exception:
                    log.warning('Ошибка получения пароля подключения')

            if decoded.get('extra'):
                if decoded.pop('is_extra_encrypted', False):
                    try:
                        decoded['extra'] = json.loads(fernet.decrypt(bytes(decoded['extra'], 'utf-8')).decode())
                    except Exception:
                        log.warning('Ошибка получения дополнительных параметров подключения')
                else:
                    decoded['extra'] = json.loads(decoded['extra'])
            else:
                decoded['extra'] = dict()

    return decoded


def get_decoded_host(encoded=None, postfix=''):
    """ Возвращает host, из параметров подключения Airflow Connection, если они есть.
    """
    return get_decoded_connection(encoded=encoded, postfix=postfix)['host']


def get_db_dialect(dialect='', engine=None, postfix='', decoded_connection=None):
    if engine:
        dialect = engine.name
    elif decoded_connection or os.getenv(f'AF_DWH_DB_CONNECTION{fix_postfix(postfix)}'):
        conn = decoded_connection or get_decoded_connection()

        if conn.get('extra', dict()).get('dialect'):
            dialect = conn['extra']['dialect']
        elif conn.get('type'):
            dialect = conn['type']

    if not dialect:
        dialect = os.getenv(f'AF_DWH_DB_DIALECT{fix_postfix(postfix)}', default='')

    return dialect.lower()


def get_conn_parameters(user=None, password=None, dialect=None, name=None,
                        server=None, port=None, connection=None, postfix=''):
    """ Возвращает словарь параметров подключения.
        Не поддерживает устаревший способ подключения через DSN.
        :postfix - роль подключения, например, PRODUCER или CONSUMER
    """
    postfix = fix_postfix(postfix)

    conn = get_decoded_connection(encoded=connection or os.getenv(f'AF_DWH_DB_CONNECTION{postfix}'), postfix=postfix)

    if not conn:
        # вернулся пустой словарь Airflow Connection, используем переменные окружения
        conn['login'] = user or os.getenv(f'AF_DWH_DB_USER{postfix}')
        conn['pass'] = password or os.getenv(f'AF_DWH_DB_PASSWORD{postfix}')
        conn['type'] = get_db_dialect(dialect=dialect, postfix=postfix)
        conn['host'] = server or os.getenv(f'AF_DWH_DB_SERVER{postfix}')
        conn['port'] = port or os.getenv(f'AF_DWH_DB_PORT{postfix}')
        conn['schema'] = name or os.getenv(f'AF_DWH_DB_NAME{postfix}')

    return conn


def get_conn_string(user=None, password=None, dialect=None, dsn=None, name=None,
                    server=None, port=None, connection=None, postfix=''):
    """ Возвращает строку коннекта к БД.
        Не переданные аргументы забираются из переменных окружения.
        connection определяет общую строку подключения и имеет приоритет над остальными параметрами.
    """
    conn = get_conn_parameters(user=user, password=password, dialect=dialect,
                               name=name, server=server, port=port,
                               connection=connection, postfix=postfix)

    db_user = conn['login']
    db_pass = conn['pass']
    db_dialect = conn['type']
    db_server = conn['host']
    db_port = conn['port']
    db_name = conn['schema']
    extra = conn.get('extra', dict())

    if db_dialect == 'mssql':
        db_dsn = dsn or os.getenv(f'AF_DWH_DB_DSN{fix_postfix(postfix)}')
        if db_dsn:
            # MSSQL ODBC DSN connection
            conn_string = f'{db_dialect}+pyodbc://{db_user}:{db_pass}@{db_dsn}'
            conn_string_secure = f'{db_dialect}+pyodbc://{db_user}:******@{db_dsn}'
        else:
            db_port = db_port or MSSQL_PORT
            driver = 'ODBC Driver 17 for SQL Server'
            extra = get_ms_extra_str(extra) if extra else ''
            params = (f'DRIVER={driver};SERVER={db_server};PORT={db_port};DATABASE={db_name};UID={db_user};'
                      f'PWD={db_pass}{extra}')
            conn_string = f'{db_dialect}+pyodbc:///?odbc_connect={quote_plus(params)}'
            params = (f'DRIVER={driver};SERVER={db_server};PORT={db_port};DATABASE={db_name};UID={db_user};'
                      f'PWD=******{extra}')
            conn_string_secure = f'{db_dialect}+pyodbc:///?odbc_connect={quote_plus(params)}'
    elif db_dialect == 'sqlite':
        if not db_name:
            # in memory sqlite db
            conn_string = f'{db_dialect}://'
        else:
            conn_string = f'{db_dialect}:///{db_name}'
        conn_string_secure = conn_string
    elif db_dialect == 'postgresql':
        db_port = db_port or POSTGRES_PORT
        conn_string = f'{db_dialect}://{db_user}:{db_pass}@{db_server}:{db_port}/{db_name}'
        conn_string_secure = f'{db_dialect}://{db_user}:******@{db_server}:{db_port}/{db_name}'
    elif db_dialect == 'qhb':
        db_port = db_port or POSTGRES_PORT
        conn_string = f'qhb+psycopg2://{db_user}:{db_pass}@{db_server}:{db_port}/{db_name}'
        conn_string_secure = f'qhb+psycopg2://{db_user}:******@{db_server}:{db_port}/{db_name}'
    else:
        raise DFOperatorException(f'Диалект {db_dialect} не поддерживается')

    log.debug(f'Строка подключения к БД: {conn_string_secure}')

    return conn_string


def get_ms_extra_str(extra):
    extra_str = []
    for key, value in extra.items():
        if option := MSSQL_OPTION_MAPPER.get(key):
            extra_str.append(f'{option}={value}')
    return ';' + ';'.join(extra_str) if extra_str else ''


def get_script_function(script_path):
    """ Подключает пользовательский модуль из файла и возвращает указанную в ней функцию """
    func = None
    script, function_name = get_script_with_function(script_path) if script_path else (None, None)
    if script and function_name:
        local_binds = {}
        binds = get_binds(local_binds=local_binds)
        rendered = get_rendered_template(script, binds=binds)
        log.info(f'Подключается функция {function_name} из <{script}>')
        log.debug(f'Скрипт:\n{rendered}')

        try:
            gl = dict()
            exec(rendered, gl)
            func = gl[function_name]
        except Exception as e:
            log.error(f'Произошла ошибка при подключениии файла скрипта: {e}')
            raise e

    return func


def get_binds(local_binds=None, postfix='', extra_env=None):
    """ Возвращает набор переменных привязки для Jinja.
        Переданные локальные переменные + переменные окружения.
        Переменные окружения должны начинаться на `AF_`
    Args:
        :local_binds - словарь локальных переменных привязки, если есть
        :postfix - постфиксы ключей нестандартных переменных
        :extra_env - переменные, дополняеющие, но не переопределяющие окружение системы
    Returns:
        итоговый словарь переменных привязки (local + env)
    """
    # все релевантные переменные окружения
    binds = extra_env or dict()
    binds.update({k: v for k, v in os.environ.items() if k.startswith('AF_')})
    # диалект БД может прийти как напрямую, так и из Airflow Connection
    binds[f'AF_DWH_DB_DIALECT{fix_postfix(postfix)}'] = get_db_dialect()
    # базовые схемы
    df_schema = get_df_schema()
    binds.update({'AF_S_': '',
                  'AF_SC_': '',
                  'AF_SDF_': f'{df_schema}.' if df_schema else ''})

    s_prefix = DEFAULT_SCHEMA_ENV

    for ev in [k for k in binds if k.startswith(s_prefix)]:
        schema = binds[ev]
        s_postfix = ev.replace(s_prefix, '')
        schema_dotted = f'{schema}.' if schema else ''
        schema = get_clean_schema(schema, env_var=False)
        schema_clean_dotted = f'{schema}.' if schema else ''
        binds.update({f'AF_S{s_postfix}_': schema_dotted, f'AF_SC{s_postfix}_': schema_clean_dotted})

    binds.update(local_binds or dict())
    return binds


def get_rendered_template(target: Union[str, Path], binds=None, constants=None, render_jinja=True,
                          ignore_jinja_errors=True, translator=None):
    """ Возвращает результат подстановки связанных переменных в шаблон.
        Умеет обрабатывать как пути, так и уже загруженные из файлов данные.
    """
    if target is None:
        raise DFOperatorException(f'Шаблон Jinja/jsql не задан.')

    # noinspection PyBroadException
    try:
        path = PosixPath(target)
    except Exception:  # текст из которого не можем получить путь
        raw = target
    else:
        # noinspection PyBroadException
        try:
            raw = path.read_text()  # текст из которого можем получить путь, но нет такого файла и т.п
        except Exception:
            raw = target

    if translator is None:
        translator = JSQL_TRANSLATOR

    tokenizer = Tokenizer(constants=constants, translator=translator)

    dumped = raw
    if render_jinja:
        try:
            dumped = Template(raw).render(binds) if binds else raw
        except Exception as je:
            log.error(f'Ошибка обработки шаблона:\n{je}')
            if not ignore_jinja_errors:
                raise je
    else:
        dumped = json.dumps(raw)
    rendered = tokenizer.stringify(dumped)

    log.debug(f'Конечный шаблон из {path}:\n{rendered}')

    return rendered


def is_item_ignored(path, ignore_file='.dfignore'):
    """ Фильтрует файлы на основе масок в .dfignore из директории с файлом. """
    root = path.parent
    pattern_file = PosixPath(root, ignore_file)
    if pattern_file.exists():
        with open(pattern_file) as f:
            spec = PathSpec.from_lines('gitwildmatch', f)

        if spec.match_file(str(path)):
            return True

    return False


def walk_level(some_dir, level=0, walk_obj=os):
    """ Итератор результатов walk_obj.walk с ограничением по глубине. """
    some_dir = str(some_dir)
    some_dir = some_dir.rstrip(walk_obj.sep)
    assert walk_obj.path.isdir(some_dir), '\'{}\' не является корректной папкой'.format(some_dir)
    num_sep = some_dir.count(walk_obj.sep)
    for root, dirs, files in walk_obj.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(walk_obj.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]


def walker(path, level=0, walk_obj=os):
    """ Возвращает дерево объектов файловой системы с учетом ограничения максимальной глубины рекурсии """
    return walk_level(path, level - 1, walk_obj=walk_obj) if level > 0 else walk_obj.walk(path)


def get_files(path, reg_exp=None, recursive=False, level=0):
    """ Возвращает файлы с путями, с учетом маски имени файла.
        level - глубина рекурсии, при 0 глубина не ограничена.
    """
    path = PosixPath(path)

    if path.exists():
        if path.is_dir():
            reg_exp = re.compile(reg_exp or r'.*')
            if recursive:
                files = list()
                for root, __, all_files in walker(path, level=level):
                    for file in all_files:
                        if reg_exp.match(file):
                            files.append(PosixPath(root, file))
            else:
                matched_objects = (i for i in path.glob(r'**/*') if reg_exp.match(i.name))
                files = sorted((file for file in matched_objects if file.is_file()))
        elif path.is_file():
            files = (path, )
        else:
            raise DFOperatorException(f'Путь {path} должен быть файлом или директорией')
    else:
        raise DFOperatorException(f'Путь {path} не существует или к нему нет доступа.')

    return [i for i in files if not is_item_ignored(i)]


def get_scripts(script_path=None, script_re=None):
    """ Возвращает содержимое файла заданного скрипта. """
    af_script_path = 'AF_SCRIPT_PATH'

    if not script_path:
        script_path = os.getenv(af_script_path)
    if not script_re:
        script_re = os.getenv('AF_SCRIPT_RE')

    if not script_path:
        msg = f'Не определена переменная окружения {af_script_path}, отсутствует путь для поиска скриптов.'
        raise DFOperatorException(msg)

    return get_files(script_path, reg_exp=script_re)


def get_script_with_function(script_path):
    """ Возвращает путь до файла и имя функции из нее, если файл имеет расширени *.pyj, а функция указана через :: """

    script, func_name = _parse_script_path(script_path)
    path = PosixPath(script)
    if not path.exists():
        raise DFOperatorException(f'Путь {path} не существует или к нему нет доступа.')
    if not path.is_file():
        raise DFOperatorException(f'Путь {path} должен быть файлом или директорией')

    return script, func_name


def _parse_script_path(script_path):
    """ Парсит путь до скрипта, который может быть указан вместе с названием функции (по аналогии с pytest)), и
    возвращает путь и имя функции. Например: '/app/tests/.py::test_unit.py::filter_files'
    """
    script, func_name = script_path, None
    tail = '::'
    if tail in script_path:
        script = script_path[:(script_path.rindex(tail) + len(tail) - 2)]
        func_name = script_path[-(len(script_path) - len(script) - 2):]
    return script, func_name


def get_cdm(path, binds=None):
    """ Возвращает модель CDM из файла. """
    if not path:
        raise DFOperatorException(f'На задан путь метаданных.')

    model_json = get_rendered_template(path, binds=binds)
    model = Model.fromJson(model_json)
    model.validate(allowUnresolvedModelReferences=False)

    return model


def clean_dir(folder):
    """ Удаляет файлы и папки из указанной директории """
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        delete_file(file_path)


def delete_file(file_path):
    """ Удаляет файл или папку """
    result = False
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
        result = True
    except Exception as e:
        log.error(f'Не удалось удалить {file_path}: {e}')
    return result


def delete_empty_dirs(folder, remove_root=True):
    """ Уаляет пустые папки из указанной директории"""
    if not os.path.isdir(folder):
        return

    # Удаляем пустые подпапки
    files = os.listdir(folder)
    if len(files):
        for f in files:
            full_path = os.path.join(folder, f)
            if os.path.isdir(full_path):
                delete_empty_dirs(full_path)

    # Если папка пустая, удаляем ее
    files = os.listdir(folder)
    if len(files) == 0 and remove_root:
        try:
            os.rmdir(folder)
        except Exception as e:
            log.error(f'Не удалось удалить {folder}: {e}')
        log.info(f'Удалена пустая папка: {folder}')


def unpack_file(file_path, extract_dir):
    """ Распаковывает архив в указанную директорию """
    result = False
    try:
        if tarfile.is_tarfile(file_path) or zipfile.is_zipfile(file_path):
            shutil.unpack_archive(file_path, extract_dir)
            result = True
    except Exception as e:
        log.error(f'Не удалось распаковать {file_path}: {e}')
    return result


def get_prop_by_path(obj, attributes, default=None):
    """ Возвращает значение цепочки свойств объекта.
        Args:
            :obj - объект, у которого нужно получить значение свойства
            :attributes - перечисление свойств в виде строк, в порядке получения
        Returns:
            значение конечного свойства либо default
    """
    for attribute in attributes:
        current_result = getattr(obj, attribute, None)
        if current_result:
            obj = current_result
        else:
            break

    result = default if current_result is None else str(current_result)

    return result


def get_as_int(value, default=0):
    try:
        value = int(value)
    except (ValueError, TypeError):
        value = default
    return value


def boolify(value):
    """ Преобразует специальные строковые значения к булеву типу. """
    if not value or value in ('False', 'None'):
        return False

    return True


def to_caseinsensitive(dictionary: dict):
    return {k.lower() if isinstance(k, str) else k:v for k,v in dictionary.items()}


def get_overridden_value(dictionary, key,
                         default=None,
                         prefix='AF_',
                         override=True,
                         sensecase=False,
                         lowered=False,
                         pop=False):
    """ Возвращает значение словаря по переданному ключу с переопределением его соответствующей переменной окружения.
        Может использоваться для произвольных словарей, например - Airflow Connection Extra.
        Args:
            :dictionary - исходный словарь (не модифицируется)
            :key - ключ искомого значения
            :default - значение, возвращаемое, если искомое значение не найдено
            :prefix - str, добавляется при поиске переменной окружения к наименованию опции
            :override - boolean, при True значение может быть переопределено соответствующей переменной окружения
            :sensecase - boolean, при True ключи опций регистрозависимые
            :lowered - boolean, при True регистронезавимые опции не приводятся к нижнему регистру для мягкого поиска,
                       т.е. предполагается, что приведение уже выполнено
            :pop - boolean, при True вырезает искомый ключ из словаря
    """
    if not sensecase and not lowered:
        dictionary = to_caseinsensitive(dictionary)

    variable = f'{prefix}{key}'.upper()

    if override:
        value = os.getenv(variable, dictionary.get(key, default))
    else:
        value = dictionary.pop(key, default) if pop else dictionary.get(key, default)

    return value


def clean_path(path):
    relative = './'
    clean = path.strip(relative)

    if relative in clean:
        DFOperatorException(f'Путь {path} некорректен')

    return clean


def sort_dataset_values(values_dict, ordered_keys):
    result = list()
    for k in ordered_keys:
        result.append(values_dict[k])
    return result


def get_ds(default=''):
    return os.getenv('AF_EXECUTION_DATE', default=default)


def get_parent_dag_id(default=''):
    return os.getenv('AF_PDAG_ID', default=default)


def get_dag_id(default=''):
    return os.getenv('AF_DAG_ID', default=default)


def get_task_id(default=''):
    return os.getenv('AF_TASK_ID', default=default)


def get_run_id(default=''):
    return os.getenv('AF_RUN_ID', default=default)


def get_job_id(default=''):
    job_id = os.getenv('AF_JOB_ID', default=default)
    return None if job_id == 'None' else job_id


def get_params(default=None):
    return os.getenv('AF_PARAMS', default=default)
