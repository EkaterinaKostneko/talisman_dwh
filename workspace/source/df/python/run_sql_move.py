"""
Created on 21 Jan 2020

@author: pymancer
"""

from os import getenv
from uuid import uuid4
from psycopg2.extras import execute_values
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import (get_binds, get_rendered_template,  # @UnresolvedImport
                                       get_scripts, get_conn_parameters, get_db_dialect)  # @UnresolvedImport
from df.common.helpers.sql import connection_scope, Op  # @UnresolvedImport

log = Logger()


def move(conn_type, cursor, data, script, batch=1000):
    if conn_type == 'postgresql':
        execute_values(cursor,
                       script,
                       data,
                       page_size=batch)
    elif conn_type in ('mssql', 'oracle', 'firebird'):
        cursor.executemany(script, data)
    else:
        raise DFOperatorException(f'Диалект {conn_type} не поддерживается')

producer_conn = get_conn_parameters(postfix='PRODUCER')
producer_type = get_db_dialect(decoded_connection=producer_conn)
consumer_conn = get_conn_parameters(postfix='CONSUMER')
consumer_type = get_db_dialect(decoded_connection=consumer_conn)

op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}

producer_env = {'AF_DWH_DB_SCHEMA_PRODUCER': producer_conn.get('extra', dict()).get('schema')}
consumer_env = {'AF_DWH_DB_SCHEMA_CONSUMER': consumer_conn.get('extra', dict()).get('schema')}

producer_binds = get_binds(local_binds=local_binds, postfix='PRODUCER', extra_env=producer_env)
consumer_binds = get_binds(local_binds=local_binds, postfix='CONSUMER', extra_env=consumer_env)

producer_script = get_scripts(script_path=getenv('AF_PRODUCER_SCRIPT_PATH'))[0]
consumer_script = get_scripts(script_path=getenv('AF_CONSUMER_SCRIPT_PATH'))[0]

producer_rendered = get_rendered_template(producer_script, binds=producer_binds)
log.debug(f'Скрипт источника:\n{producer_rendered}')

consumer_rendered = get_rendered_template(consumer_script, binds=consumer_binds)
log.debug(f'Скрипт получателя:\n{consumer_rendered}')

producer_batch = getenv('AF_PRODUCER_BATCH_SIZE')
producer_batch = int(producer_batch) if producer_batch else None
commit = False if producer_batch else True

consumer_batch = int(getenv('AF_BATCH_SIZE', 1000))

with connection_scope(dialect=producer_type,
                      server=producer_conn['host'],
                      db=producer_conn['schema'],
                      user=producer_conn['login'],
                      password=producer_conn['pass'],
                      port=producer_conn['port'],
                      extra=producer_conn.get('extra', dict())) as producer:
    with connection_scope(dialect=consumer_type,
                          server=consumer_conn['host'],
                          db=consumer_conn['schema'],
                          user=consumer_conn['login'],
                          password=consumer_conn['pass'],
                          port=consumer_conn['port'],
                          commit=commit,
                          extra=consumer_conn.get('extra', dict())) as consumer:

        if producer_batch and producer_type == 'postgresql':
            producer_cursor = producer.cursor(name=f'cursor_{uuid4().hex[:8]}')
            producer_cursor.itersize = producer_batch
        else:
            producer_cursor = producer.cursor()

        log.info('Получение данных...')
        producer_cursor.execute(producer_rendered)
        log.debug('Данные получены')

        consumer_cursor = consumer.cursor()

        if consumer_type == 'mssql' and not producer_batch:
            # с включенным fast_executemany любой fetch из курсора ломает кодировку
            consumer_cursor.fast_executemany = True

        rowcount = -1

        if producer_batch:
            log.info(f'Загружаем данные в {consumer_type} пакетами {producer_batch}/{consumer_batch}')
            batch = 0
            producer_cursor.arraysize = producer_batch

            try:
                while True:
                    batch += 1
                    log.debug(f'Пакет: {batch}')
                    data = producer_cursor.fetchmany()
                    if data:
                        move(conn_type=consumer_type,
                             cursor=consumer_cursor,
                             data=data,
                             script=consumer_rendered,
                             batch=consumer_batch)
                    else:
                        rowcount = (batch - 1) * producer_batch
                        break
            except Exception as e:
                consumer.rollback()
                raise DFOperatorException(f'Ошибка вставки данных: {e.__class__.__name__} {e}')
            else:
                consumer.commit()

        else:
            log.info(f'Загружаем данные в {consumer_type} пакетами по {consumer_batch}')
            move(conn_type=consumer_type,
                 cursor=consumer_cursor,
                 data=producer_cursor,
                 script=consumer_rendered,
                 batch=consumer_batch)

            rowcount = producer_cursor.rowcount

        producer_cursor.close()
        consumer_cursor.close()

        if rowcount and rowcount != -1:
            log.info(f'Обработано строк в пределах: {rowcount}')

op.finish()
