"""
Created on Mar 25, 2020

@author: pymancer@gmail.com
"""
from os import getenv
from uuid import uuid4
from abc import ABCMeta, abstractmethod
from psycopg2.extras import execute_values

from df.common.constants import SYSTEM_SCHEMA_ENV
from df.common.exceptions import DFAdapterException  # @UnresolvedImport
from df.common.helpers.general import (get_binds, get_rendered_template,  # @UnresolvedImport
                                       get_scripts, get_conn_parameters, get_db_dialect,  # @UnresolvedImport
                                       get_df_connection, get_df_schema, boolify)  # @UnresolvedImport
from df.common.helpers.sql import connection_scope, Op  # @UnresolvedImport
from df.common.helpers.context import ctx  # @UnresolvedImport
from df.common.helpers.logger import Logger

CONTENT_TABLE = 'op_data'

log = Logger()


class AbstractSheet(metaclass=ABCMeta):
    def __init__(self, data, **kwargs):
        self.data = data

    @abstractmethod
    def to_array(self):
        """ Возвращает данные листа в виде списка. """


class AbstractReader(metaclass=ABCMeta):
    def __init__(self, **kwargs):
        self.stream = None
        self.sheet_stream = None
        self._sheets = None
        self._parameters = None

    @property
    @abstractmethod
    def sheets(self):
        """ Словарь "листов" данных "потока" вида {'имя|номер листа': лист}. """

    @sheets.setter
    def sheets(self, sheets):
        self._sheets = sheets

    @property
    def parameters(self):
        """ Параметры данных, должны быть актуальны на этапе считывания листа. """
        if self._parameters is None:
            self._parameters = dict()

        return self._parameters

    @parameters.setter
    def parameters(self, parameters):
        self._parameters = parameters

    @abstractmethod
    def get_stream(self, path, **kwargs):
        """ Возвращает "поток" из источника. """

    @staticmethod
    def _insert_row(conn_type, cursor, script, data, template, batch=1000):
        if conn_type == 'postgresql':
            execute_values(cursor,
                           script,
                           data,
                           template=template,
                           page_size=batch
                           )
        elif conn_type in ('mssql', 'oracle', 'firebird'):
            cursor.executemany(script, data)
        else:
            raise DFAdapterException(f'Диалект {conn_type} не поддерживается')

    def _insert_data(self, script, rows, columns=None, table=CONTENT_TABLE):
        """ Разовая вставка переданных данных в поля указанной таблицы:
        script - пользовательский скрипт обрабатывающий переданные строки и возвращающий результат
        rows - данные готовые для вставки;
        columns - набор полей для вставки в таблицу, каждый адаптер может возвращать свой набор данных в зависимости
        от типа данных (текстовый, бинарный, json или xml)
          по умолчанию:
            'id' - идентификатор
            'op_ins_id' - идентификатор процесса, добавившего запись (df.op_ins.id)
            'created' - дата и время добавления
            'expiration' - дата и время окончания актуальности данных, для удаления системным dag'ом очистки таблицы
            'meta' - дополнительная информация (например stat для файла)
            'value' - текстовое значение,
            'bvalue' - бинарное значение,
            'jvalue' - значение в формате json,
            'xvalue' - значение в формате xml,
          либо другой полный (достаточный для вставки) набор полей заданный пользователем,
          если набор не передан, то вставка не происходит;
        table - таблица для вставки,
          по умолчанию системная,
          если задана пользователем, то создание таблицы и наличие ее в схеме базы, указанной в подключении задача
          пользователя;
        """
        if columns:
            # В качестве сервисной схемы используем системную
            service_conn = get_conn_parameters(connection=get_df_connection())
            service_type = get_db_dialect(decoded_connection=service_conn)
            d_service_schema = get_df_schema(dotted=True, connection=get_df_connection())
            service_schema = get_df_schema(connection=get_df_connection())

            columns_str = ', '.join(columns)
            values_str = ', '.join(['%s' for _ in range(len(columns))])
            values_tmpl = f'({values_str})'
            ret_id1 = ' OUTPUT Inserted.id' if service_type == 'mssql' else ''
            ret_id2 = ' RETURNING id' if service_type != 'mssql' else ''

            if service_type == 'postgresql':
                insert_script = f'INSERT INTO {d_service_schema}{table} ({columns_str}) VALUES %s{ret_id2}'
            else:
                insert_script = (f'INSERT INTO {d_service_schema}{table} ({columns_str}){ret_id1} VALUES ({values_str})'
                                 f'{ret_id2}')

            # select
            with connection_scope(dialect=service_type,
                                  server=service_conn['host'],
                                  db=service_conn['schema'],
                                  user=service_conn['login'],
                                  password=service_conn['pass'],
                                  port=service_conn['port'],
                                  commit=True,
                                  extra=service_conn.get('extra', dict())
                                  ) as select:
                # insert
                with connection_scope(dialect=service_type,
                                      server=service_conn['host'],
                                      db=service_conn['schema'],
                                      user=service_conn['login'],
                                      password=service_conn['pass'],
                                      port=service_conn['port'],
                                      extra=service_conn.get('extra', dict())) as insert:

                    log.info('Добавление данных...')
                    insert_cursor = insert.cursor()

                    try:
                        self._insert_row(conn_type=service_type,
                                         cursor=insert_cursor,
                                         data=rows,
                                         script=insert_script,
                                         template=values_tmpl
                                         )
                    except Exception as e:
                        insert.rollback()
                        raise DFAdapterException(f'Ошибка вставки данных: {e.__class__.__name__} {e}')
                    else:
                        insert.commit()

                    if service_type == 'mssql':
                        # с включенным fast_executemany любой fetch из курсора ломает кодировку
                        insert_cursor.fast_executemany = True

                    row_id = insert_cursor.fetchone()[0]
                    log.debug(f'Данные добавлены (ID: {row_id})')

                    if service_type == 'postgresql':
                        select_cursor = select.cursor(name=f'cursor_{uuid4().hex[:8]}')
                    else:
                        select_cursor = select.cursor()

                    local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id, 'AF_OP_DATA_ID': row_id}
                    service_env = {SYSTEM_SCHEMA_ENV: service_schema}
                    service_binds = get_binds(local_binds=local_binds, extra_env=service_env)

                    df_script = get_scripts(script_path=script)[0]
                    df_rendered = get_rendered_template(df_script, binds=service_binds)
                    log.debug(f'Скрипт получения данных:\n{df_rendered}')

                    log.info('Получение данных...')
                    select_cursor.execute(df_rendered)
                    log.debug('Данные получены')

                    data = select_cursor.fetchall()
                    column_names = [desc[0] for desc in select_cursor.description]
                    rows = [column_names]
                    rows += data

                    select_cursor.close()

                    if boolify(getenv('AF_PURGE_LOADED_DATA', default=True)):
                        log.info('Удаление обработанных данных...')
                        delete_script = f'DELETE FROM {d_service_schema}{table} WHERE id = %s;'
                        try:
                            insert_cursor.execute(delete_script, (row_id,))
                        except Exception as e:
                            insert.rollback()
                            raise DFAdapterException(f'Ошибка удаления данных: {e.__class__.__name__} {e}')
                        else:
                            insert.commit()
                        log.debug(f'Данные удалены (ID: {row_id})')
                    else:
                        ctx.append(value=row_id)

                    insert_cursor.close()
        else:
            msg = ('Для использования возможности дополнительной обработки с помощью <AF_SCRIPT_PATH> необходимо '
                   'передать columns')
            log.warning(msg)
        return rows

    def get_adapted_rows(self, rows=None, columns=None, table=CONTENT_TABLE, **kwargs):
        """ Возвращает итератор строк, обработанных по какому-то правилу. """
        if rows and columns and (script := getenv('AF_SCRIPT_PATH')):
            return self._insert_data(script, rows, columns=columns, table=table)
        else:
            return self.get_rows(**kwargs)

    @abstractmethod
    def get_rows(self, **kwargs):
        """ Возвращает итератор строк. """

    def free_resources(self, **kwargs):
        """ Освобождение ресурсов, если необходимо. """
        self.stream = None
        self.sheet_stream = None
