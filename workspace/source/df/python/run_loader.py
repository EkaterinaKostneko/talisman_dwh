"""
Created on Dec 17, 2020

@author: pymancer@gmail.com
"""
import os
import re
import shutil
import json

from os import getenv
from time import sleep
from df.common.loaders import get_loader  # @UnresolvedImport
from df.common.constants import CACHE_ROOT  # @UnresolvedImport
from df.common.exceptions import DFLoaderException  # @UnresolvedImport
from df.common.helpers.logger import Logger
from df.common.helpers.general import (get_binds, get_decoded_connection, clean_path, unpack_file,  # @UnresolvedImport
                                       delete_file, delete_empty_dirs, to_caseinsensitive,  # @UnresolvedImport
                                       get_overridden_value, get_script_function)  # @UnresolvedImport
from df.common.helpers.sql import Op  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}
binds = get_binds(local_binds=local_binds)


def get_modes():
    """ Возвращает список режимов в случае одиночных или составных
        Пример переданного значения EMPTY:COPY:UNPACK:DELETE
    """

    _mode = get_overridden_value(extra, 'mode', default='copy', prefix=env_prefix, lowered=True)
    _modes = [m.strip() for m in _mode.lower().split(':')]

    if 'multipattern' in _modes and 'multicontainer' in _modes:
        raise ValueError(('Указано некорректное значение для mode: недопустимо одновременное использование режимов '
                          '\'MULTIPATTERN\' и \'MULTICONTAINER\''))
    return _modes


def get_patterns(_modes):
    """ MULTIPATTERN - передача списка наименований (масок) файлов, поиск которых осуществляется относительно CONTAINER
    """
    _pattern = get_overridden_value(extra, 'pattern', default=r'^.*$', prefix=env_prefix, lowered=True)
    if 'multipattern' in _modes:
        try:
            _patterns = json.loads(_pattern) if _pattern else ('',)
        except ValueError as e:
            log.error(f'(get_patterns): {e}')
            raise ValueError('Указано некорректное значение для pattern')
    else:
        _patterns = (_pattern,)
    return _patterns


def get_containers(_modes, _options):
    """ MULTICONTAINER - передача списка контейнеров (рабочих директорий), относительно которых выполняется поиск
        по PATTERN
    """
    _container = options.get('container')
    if 'multicontainer' in _modes:
        try:
            _containers = json.loads(_container) if _container else ('',)
        except ValueError as e:
            log.error(f'(get_containers): {e}')
            raise ValueError('Указано некорректное значение для container')
    else:
        _containers = (_container,)
    return _containers


#   Main
#
if not CACHE_ROOT.exists():
    raise DFLoaderException('Не задан том cache')

provider_conn = get_decoded_connection(encoded=getenv('AF_PROVIDER_CONNECTION'))
if not provider_conn:
    raise DFLoaderException('Не удалось получить параметры подключения к провайдеру')
provider_code = getenv('AF_PROVIDER_CONNECTION')

env_prefix = 'AF_LOADER_'
extra = to_caseinsensitive(provider_conn.get('extra', dict()))

default_storage = f'{getenv("AF_TASK_ID", "")}/{getenv("AF_RUN_ID", "")}'
storage = clean_path(get_overridden_value(extra, 'storage', default=default_storage, prefix=env_prefix, lowered=True))
storage = CACHE_ROOT.joinpath(storage)

run = get_overridden_value(extra, 'run', default='once', prefix=env_prefix, lowered=True)
run = run.strip().lower()

interval = abs(int(get_overridden_value(extra, 'interval', default=300, prefix=env_prefix, lowered=True)))

timeout = abs(int(get_overridden_value(extra, 'timeout', default=86400, prefix=env_prefix, lowered=True)))
original_timeout = timeout

modes = get_modes()

provider = get_overridden_value(extra, 'provider', prefix=env_prefix, lowered=True)
if not provider:
    raise DFLoaderException('Провайдер не определен')

login = get_overridden_value(extra, 'user', prefix=env_prefix, lowered=True) or provider_conn.get('login')
token = get_overridden_value(extra, 'token', prefix=env_prefix, lowered=True) or provider_conn.get('pass')

level = get_overridden_value(extra, 'level', prefix=env_prefix, lowered=True)
min_level = get_overridden_value(extra, 'min_level', prefix=env_prefix, lowered=True)
time_shift = get_overridden_value(extra, 'time_shift', prefix=env_prefix, lowered=True)

options = {'ssl_verify': True, 'container': '', 'descent': 1, 'login': login, 'type': extra.get('type'),
           'level': level, 'min_level': min_level, 'time_shift': time_shift}
options.update(extra.get('options', dict()))
options = {k: get_overridden_value(options, k, prefix=env_prefix, lowered=True) for k in options}

port = f':{provider_conn["port"]}' if provider_conn['port'] else ''
host = f'{provider_conn["host"]}{port}'
loader = get_loader(host, provider, token=token, **options)

# Обработчики в зависимости от режимов
if 'empty' in modes and os.path.exists(storage):
    log.info(f'Очищаем содержимое {storage}')
    shutil.rmtree(storage)

patterns = get_patterns(modes)
filter_path = get_overridden_value(extra, 'filter', prefix=env_prefix, lowered=True)
file_filter = get_script_function(filter_path)

containers = get_containers(modes, options)
for pattern in patterns:
    pattern = re.compile(pattern)

    for container in containers:
        options.update({'container': container})

        while timeout > 0:
            log.info(f'Сканирую {host} на наличие файлов по маске {pattern.pattern}')
            timeout -= interval
            found = loader.find(pattern, **options)

            if found:
                if file_filter:
                    len1 = len(found)
                    log.info(f'Применяем фильтр к списку файлов: {len1}')


                    def decor(item):
                        try:
                            return file_filter(item.get('meta'))
                        except Exception as e:
                            log.error(f'(run_loader): {e}')
                            raise Exception(f'Произошла ошибка в пользовательской функции фильтрации: {e}')

                    found = list(filter(lambda a: decor(a), found))
                    log.info(f'Отфильтровано {len1 - len(found)} файлов')

                saved = list()
                local_saved = list()
                log.info(f'Найдено объектов: {len(found)}')

                for file in found:
                    path = loader.load(file, storage)

                    if path:
                        saved.append(file)
                        local_saved.append(path)
                        log.info(f'Загружен {path}')

                if 'cut' in modes:
                    deleted = loader.delete(saved)

                    if deleted is not None:
                        log.info(f'Удалено объектов: {deleted}')

                if 'unpack' in modes:
                    for file in local_saved:
                        target_dir = os.path.dirname(file)
                        if unpack_file(file, target_dir):
                            log.info(f'Распакован {file} в {target_dir}')

                if 'delete' in modes:
                    for file in local_saved:
                        if delete_file(file):
                            log.info(f'Удален {file}')
                    delete_empty_dirs(storage)

                if run == 'wait':
                    break

                timeout = original_timeout

            if run == 'once':
                break

            sleep(interval)

op.finish()