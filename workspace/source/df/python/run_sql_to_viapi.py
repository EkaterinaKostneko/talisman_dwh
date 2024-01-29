"""
Created on Jun 16, 2020

@author: pymancer@gmail.com
"""
from os import getenv
from collections import namedtuple
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.logger import Logger
from df.common.helpers.general import (get_binds, get_conn_parameters,  # @UnresolvedImport
                                       get_rendered_template, get_scripts)  # @UnresolvedImport
from df.common.helpers.sql import connection_scope, Op  # @UnresolvedImport
from df.common.helpers.viapi import ViHook  # @UnresolvedImport

log = Logger()


def get_post_10_item(item, **kwargs):
    """ [{'id': 'name', 'path': 'folder1/folder2', 'attr1_id': attr1_value, .., 'attrN_id': attrN_value}]
        Идентификаторы должны быть числовые.
    """
    delimiter = kwargs.get('delimiter', '/')

    item_id = item[0]
    item_name = item[1]

    item_path = item[2].split(delimiter) if item[2] else list()

    attrs = list()
    for i in range(3, len(item), 2):
        attrs.append({'AttributeId': item[i], 'Value': item[i+1]})

    result = {'Id': item_id,
              'Name': item_name,
              'Path': item_path,
              'Attributes': attrs}

    return result


def get_post_20_item(item, **kwargs):
    """ {'id': 'name', 'path': 'folder1/folder2', 'attr1_id': attr1_value, .., 'attrN_id': attrN_value} """
    raise NotImplementedError


def get_filter_10_item(item):
    filter_ = {'value': item.value,
               'type': item.type,
               'condition': item.condition}

    if item.name:
        filter_['name'] = item.name

    return filter_


def get_delete_10_item(item, **kwargs):
    """ [{"value": val1, "type": "Id", "condition": "equals"},
         {"value": val2, "type": "level",  "condition": "equals", "name": "folder1"}]
    """
    return get_filter_10_item(item)


def get_delete_20_item(item, **kwargs):
    raise NotImplementedError


def get_put_10_item(item, **kwargs):
    """ [{"value": 9998, "type": "Id", "condition": "equals", "n_value": "test1n", "n_type": "name"},
         {"value": 9998, "type": "Id", "condition": "equals", "n_value": "test1a", "n_type": "attribute", "n_name": "Номер проекта"},
         {"value": "test2", "type": "attribute", "name": "Номер проекта", "condition": "equals", "n_value": "test2n", "n_type": "name"},
         {"value": "test2", "type": "attribute", "name": "Номер проекта", "condition": "equals", "n_value": "test2a", "n_type": "attribute", "n_name": "Номер проекта"}]
    """
    filter_ = get_filter_10_item(item)

    field = {'value': item.n_value,
             'type': item.n_type}

    if item.n_name:
        field['name'] = item.n_name

    return {'filter': filter_, 'field': field}


def get_put_20_item(item, **kwargs):
    raise NotImplementedError


def get_method(operation):
    operations = {'create': 'post',
                  'delete': 'delete',
                  'update': 'put'}

    method = operations.get(operation.lower())
    if not method:
        raise DFOperatorException(f'Операция {operation} не поддерживается')

    return method


def get_itemizer(api_version='1.0', operation='create'):
    supported = {('1.0', 'post'): get_post_10_item,
                 ('1.0', 'delete'): get_delete_10_item,
                 ('1.0', 'put'): get_put_10_item}

    method = get_method(operation)
    itemizer = supported.get((api_version, method))

    if not itemizer:
        raise DFOperatorException(f'Метод {method} версии API {api_version} не поддерживается')

    return itemizer


def send_data(vihook, items, resource, method='post'):
    if method == 'post':
        data = items
    elif method == 'delete':
        filter_type = getenv('AF_FILTER_TYPE', 'and').lower()
        data = {'operation': filter_type, 'filters': items}
    elif method == 'put':
        # убираем повторяющиеся элементы
        reduced = dict()
        for i in items:
            key = f"{i['filter']['value']}{i['filter']['type']}{i['filter']['condition']}{i['filter'].get('name', '')}"

            if key in reduced:
                reduced[key]['fields'].append(i['field'])
            else:
                reduced[key] = {'filter': i['filter'], 'fields': [i['field']]}

        data = list(reduced.values())

    result = vihook.call(resource=resource, data=data, method=method)
    log.info(f'Ответ сервера: {result}')


op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}
binds = get_binds(local_binds=local_binds, postfix='PRODUCER')

script = get_scripts(script_path=getenv('AF_SCRIPT_PATH'))[0]
rendered = get_rendered_template(script, binds=binds)
log.debug(f'Скрипт источника:\n{rendered}')

producer_conn = get_conn_parameters(postfix='PRODUCER')
consumer_conn = get_conn_parameters(postfix='CONSUMER')

extra = consumer_conn['extra'] or dict()
batch = int(getenv('AF_BATCH_SIZE', 1000))
delimiter = getenv('AF_DELIMITER', '/')
api_version = extra.get('api_version', '1.0')
operation = getenv('AF_OPERATION', 'create')
resource = getenv('AF_ENDPOINT')
producer_dialect = producer_conn['type']

get_item = get_itemizer(api_version=api_version, operation=operation)

vihook = ViHook(consumer_conn['login'],
                consumer_conn['pass'],
                consumer_conn['host'],
                api_version=api_version,
                extra=extra)

with connection_scope(dialect=producer_dialect,
                      server=producer_conn['host'],
                      db=producer_conn['schema'],
                      user=producer_conn['login'],
                      password=producer_conn['pass'],
                      port=producer_conn['port'],
                      extra=producer_conn.get('extra', dict())) as producer:
    cursor = producer.cursor()
    log.info('Получаем данные...')
    cursor.execute(rendered)

    log.info(f'Загружаем данные...')

    if producer_dialect == 'postgresql':
        # создаем класс строки, т.к. строки курсора постгреса не содержат имен атрибутов
        NamedRow = namedtuple('NamedRow',
                              [f'c{i}' if c.name == '?column?' else c.name for i, c in enumerate(cursor.description)]
                              )

    while True:
        items = list()
        rows = cursor.fetchmany(batch)
        if rows:
            for row in rows:
                if producer_dialect == 'postgresql':
                    # добавляем имена атрибутам строки
                    row = NamedRow(*row)

                item = get_item(row, delimiter=delimiter)
                items.append(item)
                
            try:
                send_data(vihook=vihook, resource=resource, items=items, method=get_method(operation))
            except Exception as e:
                log.debug(f'Ошибка <{e.__class__.__name__} {e}> при отправке даннных:\n{items}')
                raise DFOperatorException(f'')
        else:
            break

op.finish()
