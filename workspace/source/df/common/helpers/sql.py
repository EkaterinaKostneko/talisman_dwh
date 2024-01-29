'''
Created on 23 Jul 2019

@author: pymancer
'''
import uuid
import json
import os.path
import pyodbc
import psycopg2
import firebirdsql
import xml.etree.ElementTree as etree

from ast import literal_eval
from datetime import datetime
from contextlib import contextmanager
from sqlalchemy import (text as textsql, create_engine, MetaData, select, func,
                        ForeignKey, Table, Column,
                        Integer, Unicode, Date, DateTime, UnicodeText, Float,
                        UniqueConstraint, VARCHAR, CHAR, BOOLEAN, DECIMAL)
from sqlalchemy.orm import sessionmaker, mapper, clear_mappers
from sqlalchemy.sql import expression
from sqlalchemy.exc import ProgrammingError, InvalidRequestError, IntegrityError
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.types import TypeDecorator
from sqlalchemy.schema import CreateTable, Sequence
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mssql import BIT, UNIQUEIDENTIFIER, DATETIME2, XML
from sqlalchemy.dialects.postgresql import UUID, JSON
from alembic import op

from df.common.schemas.model import DataType, LocalEntity, ReferenceEntity  # @UnresolvedImport
from df.common.constants import (POSTGRES_PORT, MSSQL_PORT,  # @UnresolvedImport
                                 ORACLE_PORT, FIREBIRD_PORT, UNDEFINED)  # @UnresolvedImport
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import (get_conn_string, get_ms_extra_str, get_cdm, get_db_schema,  # @UnresolvedImport
                                       get_oracle_driver, get_df_schema, get_clean_schema,  # @UnresolvedImport
                                       get_ds, get_dag_id, get_task_id, get_run_id,  # @UnresolvedImport
                                       get_parent_dag_id, get_job_id, get_params,  # @UnresolvedImport
                                       get_df_connection)  # @UnresolvedImport
from df.common.helpers.context import ctx  # @UnresolvedImport

log = Logger()


class GUID(TypeDecorator):
    """ Platform-independent GUID type.
        Uses PostgreSQL's UUID type, otherwise uses
        CHAR(32)*, storing as stringified hex values.
        https://docs.sqlalchemy.org/en/13/core/custom_types.html
        *char(36), since MS keeps dashes.
    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name in ('postgresql', 'qhb'):
            r = dialect.type_descriptor(UUID())
        elif dialect.name == 'mssql':
            r = dialect.type_descriptor(CHAR(36))
        else:
            r = dialect.type_descriptor(UNIQUEIDENTIFIER())
        return r

    def process_bind_param(self, value, dialect):
        if value is None:
            r = value
        elif dialect.name in ('postgresql', 'mssql', 'qhb'):
            r = str(value)
        else:
            if isinstance(value, uuid.UUID):
                # hexstring
                r = "%.32x" % value.int
            else:
                r = "%.32x" % uuid.UUID(value).int
        return r

    def process_result_value(self, value, dialect):
        # `dialect` не убирать и не переименовывать.
        if value is None:
            r = value
        else:
            if not isinstance(value, uuid.UUID):
                r = uuid.UUID(value)

        return r


class IJSON(TypeDecorator):
    """ Platform-independent JSON type.
        Uses JSON type for PostgreSQL, Unicode(None) for MSSQL. 
    """
    impl = JSON

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mssql':
            return dialect.type_descriptor(Unicode(None))
        else:
            return dialect.type_descriptor(JSON())

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)

        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                value = None

        return value


class XMLType(TypeDecorator):
    impl = UnicodeText
    type = etree.Element

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mssql':
            return dialect.type_descriptor(XML())
        else:
            return dialect.type_descriptor(UnicodeText())

    @staticmethod
    def get_col_spec():
        return 'xml'

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return etree.dump(value)
            else:
                return None
        return process

    def process_result_value(self, value, dialect):
        if value is not None:
            value = etree.fromstring(value)
        return value


class UTCNow(expression.FunctionElement):
    type = DateTime()


class UTCNowMS(expression.FunctionElement):
    """ MSSQL Server datetime2 UTCNow version """
    type = DATETIME2()


class UTCDateTime(expression.FunctionElement):
    type = DateTime()


class UTCDate(expression.FunctionElement):
    type = Date()


class RID(expression.FunctionElement):
    type = GUID()


@compiles(UTCNow, 'postgresql', 'qhb')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(UTCNow, 'mssql')
def ms_utcnow(element, compiler, **kw):
    return "GETUTCDATE()"


@compiles(UTCNowMS, 'mssql')
def ms_utcnow2(element, compiler, **kw):
    return "SYSUTCDATETIME()"


@compiles(UTCDateTime, 'postgresql', 'qhb')
def pg_utcdatetime(element, compiler, **kw):
    return "DATE_TRUNC('second', TIMEZONE('utc', CURRENT_TIMESTAMP))"


@compiles(UTCDateTime, 'mssql')
def ms_utcdatetime(element, compiler, **kw):
    return "CONVERT(VARCHAR(19), GETUTCDATE(), 120)"


@compiles(UTCDate, 'postgresql', 'qhb')
def pg_utcdate(element, compiler, **kw):
    return "DATE_TRUNC('day', TIMEZONE('utc', CURRENT_TIMESTAMP))"


@compiles(UTCDate, 'mssql')
def ms_utcdate(element, compiler, **kw):
    return "CAST(GETUTCDATE() AS DATE)"


@compiles(RID, 'postgresql', 'qhb')
def pg_rid(element, compiler, **kw):
    return "public.UUID_GENERATE_V1()"


@compiles(RID, 'mssql')
def ms_rid(element, compiler, **kw):
    return "NEWID()"


# дефолтные значения свойств столбцов таблицы
COLUMN_DEFAULTS = {'role': None,
                   'primaryKey': False,
                   'logicalKey': False,
                   'sequence': None,
                   'sequenceCache': None,
                   'default': None,
                   'nullable': True,
                   'unique': False,
                   'softNullable': True,
                   'softUnique': False,
                   'index': False,
                   'length': None,
                   'precision': None,
                   'scale': None,
                   'format': None}

# дефолтные параметры типов данных
DEFAULT_DECIMAL_PRECISION = '28'
DEFAULT_DECIMAL_SCALE = '10'
DEFAULT_FLOAT_PRECISION = '53'

# маппинг типов данных CDM и SQLAlchemy
CDM_TYPES = {DataType.String: Unicode,
             DataType.Int64: Integer,
             DataType.Varchar: VARCHAR,
             DataType.Guid: GUID,
             DataType.Text: UnicodeText,
             DataType.Date: Date,
             DataType.DateTime: DateTime,
             DataType.Timestamp: DateTime,
             DataType.Unicode: Unicode,
             DataType.Integer: Integer,
             DataType.Decimal: DECIMAL,
             DataType.Boolean: BOOLEAN,
             DataType.Float: Float}

# значения свойств столбцов таблицы по ролям
ROLE_DEFAULTS = {'id': {'dataType': CDM_TYPES[DataType.Integer], 'primaryKey': True},
                 'rid': {'dataType': CDM_TYPES[DataType.Guid], 'nullable': False, 'unique': True, 'default': RID()},
                 'sid': {'dataType': CDM_TYPES[DataType.Varchar], 'nullable': False, 'length': '128'},
                 'runid': {'dataType': CDM_TYPES[DataType.Varchar], 'nullable': False, 'length': '128', 'index': True},
                 'opid': {'dataType': CDM_TYPES[DataType.Integer], 'nullable': False, 'index': True},
                 'opinsid': {'dataType': CDM_TYPES[DataType.Integer], 'nullable': False, 'index': True},
                 'jobid': {'dataType': CDM_TYPES[DataType.Integer], 'nullable': False, 'index': True},
                 'datebegin': {'dataType': CDM_TYPES[DataType.DateTime], 'nullable': False},
                 'dateend': {'dataType': CDM_TYPES[DataType.DateTime], 'nullable': False},
                 'unitid': {'dataType': CDM_TYPES[DataType.String], 'nullable': False, 'length': '128'},
                 'cd': {'dataType': CDM_TYPES[DataType.Date], 'default': UTCDate()},
                 'cdt': {'dataType': CDM_TYPES[DataType.DateTime], 'default': UTCDateTime()},
                 'cts': {'dataType': CDM_TYPES[DataType.Timestamp], 'default': UTCNow()},
                 'code': {'dataType': CDM_TYPES[DataType.String], 'primaryKey': True, 'length': '32'},
                 'tag': {'dataType': CDM_TYPES[DataType.Integer]}
                 }


@contextmanager
def session_scope(session_class):
    """ Предоставляет контекст транзакции. """
    session = session_class()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise DFOperatorException(f'Ошибка транзакции: {e}') from e
    finally:
        session.close()


def get_ms_conn_string(server, db, user, password, port=None, extra=None):
    if not port:
        port = '1433'
    extra = get_ms_extra_str(extra) if extra else ''

    return (f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server},{port};DATABASE={db};UID={user};PWD={password}'
            f'{extra}')


def get_pg_conn_string(server, db, user, password, port=None):
    if not port:
        port = '5432'

    return f"host='{server}' port='{port}' dbname='{db}' user='{user}' password='{password}'"


@contextmanager
def connection_scope(dialect, server, db, user, password, port=None, commit=True, extra=None):
    """ Контекст одиночной транзакции (либо сводимой к таковой) с принудительным коммитом. """
    dialect = dialect.lower()
    conn = None
    try:
        log.debug(f'Соединение {dialect} с {server}...')

        if dialect == 'mssql':
            port = port or MSSQL_PORT
            conn = pyodbc.connect(get_ms_conn_string(server, db, user, password, port=port, extra=extra), autocommit=False)
            conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
            conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
            conn.setencoding(encoding='utf-8')
        elif dialect == 'postgresql':
            port = port or POSTGRES_PORT
            conn = psycopg2.connect(get_pg_conn_string(server, db, user, password, port=port))
        elif dialect == 'oracle':
            port = port or ORACLE_PORT
            server = f'{server}:{port}/{db}'
            driver = get_oracle_driver()
            conn = pyodbc.connect(**{'DBQ': server, 'uid': user, 'pwd': password, 'driver': driver})
        elif dialect == 'firebird':
            port = port or FIREBIRD_PORT
            conn = firebirdsql.connect(host=server, database=db, port=port, user=user, password=password)
        else:
            raise DFOperatorException(f'Диалект {dialect} не поддерживается')

        log.debug(f'...установлено')

        yield conn

        if commit:
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise DFOperatorException(f'Ошибка в контексте транзакции {dialect}: {e.__class__.__name__} {e}') from e
    finally:
        if conn:
            conn.close()
            log.debug(f'Соединение {dialect} с {server} закрыто')


def get_select_result(row_sql_result, with_headers=False):
    """ Возвращает результат парсинга ответа СУБД на запрос. """
    sql_result = list()
    if row_sql_result:
        try:
            sql_result = row_sql_result.fetchall()
        except Exception as e:
            log.debug(f'Результат выполнения SQL пуст. Сообщение сервера: {e}')
        else:
            if with_headers:
                sql_result.insert(0, tuple(desc[0] for desc in row_sql_result.description))
    else:
        log.debug(f'Результат выполнения SQL пуст.')

    return sql_result


def process_sql(sql, engine=None, nocount=True):
    """ Обрабатывает переданный скрипт SQL в зависимости от настроек и диалекта
    """
    if not engine:
        engine = create_engine(get_conn_string())
        engine_is_local = True
    else:
        engine_is_local = False

    if nocount and engine.name == 'mssql':
        log.debug(f'Выполняется предобработка SQL')
        sql = f'set nocount on;\n\n{sql}'

    if engine_is_local:
        engine.dispose()

    return sql


def execute_sql(sql, engine=None, connection=None, get_select=False, with_headers=False):
    """ Выполняет переданный скрипт SQL и возвращает результат, если необходимо.
        Если запрос должен выполниться в существующей транзакции (connection передан),
        то коллер должен позаботиться о коммите.
    """
    result = None
    if sql.strip():
        log.debug(f'Выполняется SQL:\n{sql}')

        if not engine and not connection:
            engine = create_engine(get_conn_string())
            engine_is_local = True
        else:
            engine_is_local = False

        if connection:
            log.debug('Выполняем sql в существующей транзакции')
            result = connection.execute(textsql(sql))

            if get_select:
                result = get_select_result(result, with_headers=with_headers)
        elif engine:
            log.debug('Выполняем sql в отдельной транзакции')
            connection = engine.raw_connection()

            try:
                cursor = connection.cursor()
                cursor.execute(sql)

                if get_select:
                    result = get_select_result(cursor, with_headers=with_headers)
                else:
                    if engine.name == 'mssql':
                        # пропускаем безобидные сообщения в поисках поглощенных ошибок
                        try:
                            while cursor.nextset():
                                cursor.fetchone()
                        except pyodbc.ProgrammingError as pe:
                            if 'Previous SQL was not a query.' not in str(pe):
                                raise pe

                connection.commit()
            except Exception as e:
                connection.rollback()
                raise e
            finally:
                connection.close()
        else:
            msg = f'Ошибочный набор параметров: engine: {engine}, get_select: {get_select}, transaction: {connection}'
            raise DFOperatorException(msg)

        if engine_is_local:
            engine.dispose()

        log.debug('Выполнение запроса SQL завершено.')
    return result


def execute_statements(statements, engine=None, connection=None, is_select=False):
    """ Выполняет переданные скрипты в отдельных сессиях, если не передан connection. """
    results = list()

    for statement in statements:
        get_select = True if is_select or 'select' in statement[:7].lower() else False

        sql_result = execute_sql(statement, engine=engine, connection=connection, get_select=get_select)

        if get_select and sql_result:
            log.debug('Возвращение результата выборки')
            for item in sql_result:
                results.append(item)

    return results


def get_attribute_annotations(cdm_attribute):
    """ Возвращает значение свойств столбца таблицы из его аннотации в модели CDM.
        Учитывает значения по умолчанию и роль атрибута.
    """
    attribute_datatype = DataType.String
    raw_annotations = cdm_attribute.get_notes()
    annotations = COLUMN_DEFAULTS.copy()
    if raw_annotations.get('role'):
        attribute_datatype = ROLE_DEFAULTS[raw_annotations['role']]['dataType']
        annotations.update(ROLE_DEFAULTS[raw_annotations['role']])
        # dataType не является аннотацией
        annotations.pop('dataType')

    annotations.update(raw_annotations)

    if cdm_attribute.dataType != DataType.Unclassified:
        # Тип поля с ролью переопределен
        attribute_datatype = cdm_attribute.dataType

    if attribute_datatype == DataType.Decimal:
        if not annotations.get('precision'):
            annotations['precision'] = DEFAULT_DECIMAL_PRECISION
        if not annotations.get('scale'):
            annotations['scale'] = DEFAULT_DECIMAL_SCALE
        annotations['length'] = f"{annotations['precision']},{annotations['scale']}"
    elif attribute_datatype == DataType.Float:
        annotations['length'] = annotations.get('precision') or DEFAULT_FLOAT_PRECISION
    elif attribute_datatype not in (DataType.String, DataType.Varchar, DataType.Unicode):
        annotations['length'] = None

    return annotations


def get_engine_metadata(engine=None, metadata=None, schema=None):
    """ Возвращает метаданные, создавая их, в случае необходимости. """
    if not (engine or metadata):
        raise DFOperatorException('Недостаточно параметров для получения метаданных БД')

    if not metadata:
        metadata = MetaData(bind=engine, schema=schema)

    return metadata


class Op:
    """ Метаданные процесса. """
    _id = None
    _ins_id = None

    def __init__(self, proc_id=None, step_id=None, params=None):
        self.proc_id = proc_id or get_dag_id()
        self.step_id = step_id or get_task_id()
        self.params = params or get_params()

        if self.proc_id:
            self.schema = get_df_schema()
            self.engine = create_engine(get_conn_string(connection=get_df_connection()))
            metadata = MetaData(bind=self.engine, schema=self.schema)
            self.session_class = sessionmaker(bind=self.engine)

            self.t_op = Table('op', metadata, autoload=True)
            self.c_op = type('Op', (), {})
            mapper(self.c_op, self.t_op)

            self.t_op_ins = Table('op_ins', metadata, autoload=True)
            self.c_op_ins = type('OpIns', (), {})
            mapper(self.c_op_ins, self.t_op_ins)

            self.t_op_ctx = Table('op_ctx', metadata, autoload=True)
            self.c_op_ctx = type('OpCtx', (), {})
            mapper(self.c_op_ctx, self.t_op_ctx)

    def __del__(self):
        try:
            self.engine.dispose()
        except Exception:
            pass

    @property
    def id(self):
        if self.proc_id and self._id is None:
            self._id = self._get_id()

        return self._id

    @property
    def ins_id(self):
        if self.proc_id and self.step_id and self._ins_id is None:
            self._ins_id = self._get_ins_id()

        return self._ins_id

    def finish(self):
        if self.proc_id and self.step_id:
            clear_mappers()
            mapper(self.c_op_ins, self.t_op_ins)
    
            with session_scope(self.session_class) as s:
                op_ins = s.query(self.c_op_ins).get(self.ins_id)
                if op_ins:
                    op_ins.ts_finish = datetime.now()

            # TODO: здесь явное нарушение зоны ответственности,
            # но не удаление нужно вынести отсюда,
            # а загрузку контекста перенести сюда
            schema = f'{self.schema}.' if self.schema else self.schema
            sql = f'''delete from {schema}entity_ctx where op_ins_id = {self.ins_id};
                  delete from {schema}relation_ctx where op_ins_id = {self.ins_id};
               '''
            execute_sql(sql, engine=self.engine)

        ctx.flush(task_id=self.step_id)

    def _get_id(self):
        """ Возвращает идентификатор соответствующей записи таблицы `op`
        - при отсутствии записи в `op` создает новую
        - при наличии параметров запуска сохраняет их в `op_ctx`
        """
        op_record_id = ''
        with session_scope(self.session_class) as s:
            run_id = get_run_id(default=None)
            op_record = s.query(self.t_op).filter_by(proc_id=self.proc_id, run_id=run_id).one_or_none()

            if op_record:
                op_record_id = op_record.id
            else:
                op_record = self.c_op()
                op_record.parent_id = get_parent_dag_id(default=None)
                op_record.proc_id = self.proc_id
                op_record.run_id = run_id
                op_record.ts = get_ds(default=None)
                try:
                    s.add(op_record)
                    s.flush()
                    op_record_id = op_record.id
                except (IntegrityError, UnmappedInstanceError) as e:  # @UndefinedVariable
                    # Ловим IntegrityError (psycopg2.errors.UniqueViolation) для op_proc_id_run_id_key и
                    #  Class 'df.common.helpers.sql.Op' is not mapped
                    log.warning(f'Запись с таким op_proc_id/run_id уже существует:\n{e}')
                    s.rollback()
                    op_record = s.query(self.t_op).filter_by(proc_id=self.proc_id, run_id=run_id).one_or_none()
                    op_record_id = op_record.id

                if self.params:
                    try:
                        params = literal_eval(self.params)
                    except Exception as e:
                        log.warning(f'Не удалось загрузить параметры процесса {e}:\n{self.params}')
                    else:
                        ctx_record = self.c_op_ctx()
                        ctx_record.op_id = op_record_id
                        ctx_record.params = params
                        s.add(ctx_record)

        return op_record_id

    def _get_ins_id(self):
        """ Возвращает идентификатор соответствующей записи таблицы `op_ins`
        - при отсутствии записи в `op_ins` создает новую
        """
        ins_record_id = ''
        with session_scope(self.session_class) as s:
            step_id = get_task_id(default=None)
            job_id = get_job_id(default=UNDEFINED)
            ins_record = s.query(self.t_op_ins).filter_by(op_id=self.id, step_id=step_id, job_id=job_id).one_or_none()

            if ins_record:
                ins_record_id = ins_record.id
            else:
                ins_record = self.c_op_ins()
                ins_record.op_id = self.id
                ins_record.step_id = step_id
                ins_record.job_id = job_id
                s.add(ins_record)
                s.flush()
                ins_record_id = ins_record.id

        return ins_record_id


def get_formatting_table(data_table_name, rid_column, metadata, schema):
    """ Возвращает таблицу для дополнительных данных.
        При необходимости ее создает.
        Имя таблицы - `<Имя таблицы с данными>_f`.
        TODO: переделать на создание через create_entity
    """
    extra_table_name = f'{data_table_name}_f'
    uix_name = f'uix_{extra_table_name}'

    fk = f'{data_table_name}.{rid_column}'

    if metadata.bind.dialect.name == 'mssql':
        guid_type = UNIQUEIDENTIFIER
    else:
        guid_type = GUID

    columns = [
        Column('id', Integer, primary_key=True),
        Column('row_id', guid_type, ForeignKey(fk, onupdate='CASCADE', ondelete='CASCADE'), nullable=False),
        Column('column_name', VARCHAR(128), nullable=False),
        Column('key', VARCHAR(128), nullable=False),
        Column('value', VARCHAR(256))
    ]

    extra_table = Table(extra_table_name, metadata, *columns,
                        UniqueConstraint('row_id', 'column_name', 'key', name=uix_name),
                        schema=schema)
    extra_table.create(checkfirst=True)
    return extra_table


def truncate_table(table, schema='', connection=None, engine=None):
    """ Чистит таблицу по имени. """
    table_full = f'{schema}."{table}"' if schema else f'"{table}"'

    if not connection:
        if not engine:
            raise DFOperatorException(f'Для удаления таблицы {table_full} необходим engine или connection.')
        connection = engine.connect()

    dialect = connection.engine.name

    if dialect in ('postgresql', 'qhb'):
        sql = f'truncate table {table_full} cascade;'
    elif dialect == 'mssql':
        sql = f'truncate table {table_full};'
    elif dialect == 'sqlite':
        sql = f'delete from {table_full}; vacuum;'

    query = textsql(sql)
    try:
        connection.execution_options(autocommit=True).execute(query)
    except ProgrammingError as e:
        # нельзя транкейтить таблицу в mssql, если на нее ссылаются fk
        table_full_f = f'{schema}."{table}_f"' if schema else f'"{table}_f"'
        log.warning(f'Используем delete после ошибки транкейта таблицы {table_full}: {e}')
        sql = f'delete from {table_full_f}; delete from {table_full};'
        query = textsql(sql)
        connection.execution_options(autocommit=True).execute(query)


def count_table_rows(table, connection, column_name=None, value=None):
    """ Подсчитывает количество строк в таблице по условию на равенство. """
    query = select([func.count()]).select_from(table)
    if column_name:
        query = query.where(getattr(table.columns, column_name) == value)
    result = connection.execute(query)
    return result.fetchone()[0]


class MetadataWalker:
    """ Проходит по связям метаданных, позволяя потомкам выполнять операции над найденными метаданными. """

    class CurrentTable:
        pass

    class LinkedTable:
        pass

    def __init__(self, engine=None, metadata=None, schema=None, binds=None, **kwargs):
        self.done_local_entities = list()
        self.done_reference_entities = list()
        self.done_relations = list()
        self.done_models = dict()
        self.done_entities_schemas = dict()
        self.binds = binds
        self.schema = get_db_schema(schema=schema)
        self.metadata = get_engine_metadata(engine=engine, metadata=metadata, schema=self.schema)
        self.session_cls = sessionmaker(bind=engine)
        # простой детектор потенциальных циклов
        self.failsafe = 1000

    def process(self, cdm_path, role=None, entity_name=None, entity_source=None, annotations=None):
        """ Обработка метаданных всех релевантных сущностей модели и их отношений. """
        if not cdm_path:
            return

        self.failsafe -= 1
        if self.failsafe < 0:
            raise DFOperatorException('Загрузка динамических метаданных зациклилась.')

        entity_source = entity_source or entity_name
        entity_source_copy = entity_source

        cdm_model = self._load_model(cdm_path)

        log.info(f'Обрабатываются метаданные модели <{cdm_model.name}>.')

        for entity in cdm_model.entities:
            if entity_source:
                if entity_source == entity.name:
                    # ссылочная модель, обрабатываем под наименованием, используемым ссылающейся моделью
                    entity.name = entity_name
                    # если у ссылочной сущности была указана аннтоация, объединяем ее с той, которая уже есть у основной
                    if annotations:
                        entity.merge_notes(annotations)
                    self._process_entity(entity, cdm_model, entity_role=role)
                    entity.name = entity_source
                    # cущность найдена по entity_source и остальные сущности в файле уже обрабатываются без внешнего
                    # entity_source, т.к. name уникально
                    entity_source = None
            else:
                self._process_entity(entity, cdm_model, entity_role=role)

        # восстанавливаем entity_source, т.к. он может использоваться при создании связей
        entity_source = entity_source_copy
        for relationship in cdm_model.relationships:
            if entity_source:
                from_entity = relationship.fromAttribute.entityName
                to_entity = relationship.toAttribute.entityName
                if entity_source in (from_entity, to_entity):
                    # ссылочная модель, обрабатываем под наименованием, используемым ссылающейся моделью
                    if entity_source == from_entity:
                        relationship.fromAttribute.entityName = entity_name
                        self._process_relationship(relationship)
                        relationship.fromAttribute.entityName = entity_source
                    else:
                        relationship.toAttribute.entityName = entity_name
                        self._process_relationship(relationship)
                        relationship.toAttribute.entityName = entity_source
            else:
                self._process_relationship(relationship)

    def _load_model(self, cdm_path):
        cdm_path = os.path.abspath(cdm_path)

        if cdm_path in self.done_models:
            cdm_model = self.done_models[cdm_path]
        else:
            log.info(f'Загружается модель из <{cdm_path}>.')
            cdm_model = get_cdm(cdm_path, binds=self.binds)
            self.done_models[cdm_path] = cdm_model

        return cdm_model

    def _process_entity(self, entity, cdm_model, entity_role=None):
        if isinstance(entity, LocalEntity):
            self._process_local_entity(entity, entity_role=entity_role)
        elif isinstance(entity, ReferenceEntity):
            self._process_reference_entity(entity, cdm_model.referenceModels, entity_role=entity_role)

    def _process_this_entity(self, local_entity, entity_role=None):
        """ Дополнительный обработчик сущности, реализованный в потомке. """
        pass

    def _process_local_entity(self, local_entity, entity_role=None):
        if local_entity.name not in self.done_local_entities:
            self.done_local_entities.append(local_entity.name)
            self._process_this_entity(local_entity, entity_role=entity_role)

    def _process_reference_entity(self, reference_entity, reference_models, entity_role=None):
        if reference_entity.name not in self.done_reference_entities:
            self.done_reference_entities.append(reference_entity.name)
            for reference_model in reference_models:
                if reference_entity.modelId == reference_model.id:
                    self.process(reference_model.location,
                                 role=entity_role,
                                 entity_name=reference_entity.name,
                                 entity_source=reference_entity.source,
                                 annotations=reference_entity.annotations)
                    break

    def _process_this_relationship(self, cdm_relationship):
        """ Дополнительный обработчик связи, реализованный в потомке. """
        pass

    def _process_relationship(self, cdm_relationship):
        cdm_relationship_hash = hash(cdm_relationship)

        if cdm_relationship_hash not in self.done_relations:
            self.done_relations.append(cdm_relationship_hash)
            self._process_this_relationship(cdm_relationship)


class MetadataLoader(MetadataWalker):
    """ Позволяет загружать метаданные в таблицы динамических метаданных.
        Идентификация динамических метаданных оператором должна выполняться по AF_OP_INS_ID.
        Загружает все метаданные из полученной модели + метаданные всех сущностей,
        на которые есть выход по связям сущностей полученной модели.
        Загружает метаданные каждой сущности в рамках одного JOB_ID только один раз.
    """
    entity_table = 'entity_ctx'
    attr_table = 'attr_ctx'
    relation_table = 'relation_ctx'

    def __init__(self, engine=None, metadata=None, schema=None, binds=None, **kwargs):
        schema = schema or get_df_schema()
        super().__init__(engine=engine, metadata=metadata, schema=schema, binds=binds, **kwargs)

        binds = binds or dict()
        self.ins_id = binds.get('AF_OP_INS_ID')

        if not self.ins_id:
            msg = f'Не определен идентификатор выполняемого шага, загрузка метаданных невозможна.'
            raise DFOperatorException(msg)

    def _process_this_entity(self, local_entity, entity_role=None):
        log.debug(f'Сущность <{local_entity}> с ролью <{entity_role}> загружается в <{self.entity_table}>.')

        with session_scope(self.session_cls) as s:
            ct = Table(self.entity_table, self.metadata, autoload=True)
            lt = Table(self.attr_table, self.metadata, autoload=True)
            clear_mappers()
            mapper(MetadataLoader.CurrentTable, ct)
            mapper(MetadataLoader.LinkedTable, lt)

            e_record = MetadataLoader.CurrentTable()
            e_record.op_ins_id = self.ins_id
            e_record.name = local_entity.name
            e_record.schema = self.schema
            e_record.role = entity_role
            s.add(e_record)
            s.flush()
            e_record_id = e_record.id

            for idx, attribute in enumerate(local_entity.attributes):
                # по умолчанию тип атрибута всегда строковый
                attribute_type = attribute.dataType if attribute.dataType != DataType.Unclassified else DataType.String
                a_record = MetadataLoader.LinkedTable()
                a_record.entity_ctx_id = e_record_id
                a_record.name = attribute.name
                a_record.ord = idx
                a_record.type = attribute_type.toJson()
                role = attribute.get_note('role')
                a_record.role = role
                annotations = get_attribute_annotations(attribute)

                a_record.unique = annotations['unique']
                a_record.nullable = annotations['nullable']
                a_record.length = annotations['length']
                a_record.format = annotations['format']
                a_record.default = annotations['default']
                a_record.pk = annotations['primaryKey']
                a_record.lk = annotations['logicalKey']

                s.add(a_record)

    def _process_this_relationship(self, cdm_relationship):
        log.debug(f'Связь <{cdm_relationship}> загружается в <{self.relation_table}>.')
        ct = Table(self.relation_table, self.metadata, autoload=True)
        clear_mappers()
        mapper(MetadataLoader.CurrentTable, ct)

        from_entity = cdm_relationship.fromAttribute.entityName
        to_entity = cdm_relationship.toAttribute.entityName

        with session_scope(self.session_cls) as s:
            r_record = MetadataLoader.CurrentTable()
            r_record.op_ins_id = self.ins_id
            r_record.from_entity = from_entity
            r_record.from_column = cdm_relationship.fromAttribute.attributeName
            r_record.to_entity = to_entity
            r_record.to_column = cdm_relationship.toAttribute.attributeName

            s.add(r_record)


class MetadataWarlock(MetadataWalker):
    """ Создает таблицы в БД по связанным метаданным. """
    def _process_this_entity(self, local_entity, entity_role=None):
        schema = local_entity.get_note('schema', default=self.schema)
        self.done_entities_schemas[local_entity.name] = schema  # имя сущности считаеется уникальным
        self.create_entity(local_entity, metadata=self.metadata, schema=schema)

    def _process_this_relationship(self, cdm_relationship):
        self.create_relationship(cdm_relationship, metadata=self.metadata, schema=self.schema,
                            entities_schemas=self.done_entities_schemas)

    @staticmethod
    def create_entity(entity, engine=None, metadata=None, schema=None):
        """ Создает и возвращает таблицу на основе метаданных CDM. """
        if isinstance(entity, LocalEntity):
            metadata = get_engine_metadata(engine=engine, metadata=metadata, schema=schema)
            dialect = metadata.bind.dialect.name

            log.debug(f'Обрабатывается сущность {entity.name}.')

            table_exists = False
            ignore_existing_table = entity.get_note('ignoreExistingTable', default=True)

            if ignore_existing_table:
                try:
                    metadata.reflect(only=(entity.name,), schema=schema)
                except InvalidRequestError:
                    table_exists = False
                else:
                    table_exists = True

            if table_exists:
                table = Table(entity.name, metadata, autoload=True, schema=schema)
                log.debug(f'Сущность {entity.name} уже существует.')
            else:
                columns = []
                for attribute in entity.attributes:
                    annotations = get_attribute_annotations(attribute)

                    if attribute.dataType in (DataType.String, DataType.Varchar, DataType.Unicode):
                        length = int(annotations['length']) if annotations['length'] else None
                        column_type = CDM_TYPES[attribute.dataType](length)
                    elif attribute.dataType == DataType.Decimal:
                        precision = int(annotations['precision']) if annotations['precision'] else None
                        scale = int(annotations['scale']) if annotations['scale'] else None
                        column_type = CDM_TYPES[attribute.dataType](precision=precision, scale=scale)
                    elif attribute.dataType == DataType.Float:
                        precision = int(annotations['precision']) if annotations['precision'] else None
                        column_type = CDM_TYPES[attribute.dataType](precision=precision)
                    elif attribute.dataType == DataType.Boolean and dialect == 'mssql':
                        column_type = BIT
                    elif attribute.dataType == DataType.Guid and dialect == 'mssql':
                        column_type = UNIQUEIDENTIFIER
                    else:
                        # по умолчанию тип атрибута всегда строковый
                        column_type = CDM_TYPES.get(attribute.dataType, DataType.String)

                    if annotations.get('softUnique', False):
                        annotations['unique'] = False
                    if annotations.get('softNullable', False):
                        annotations['nullable'] = True

                    column_args = [attribute.name, column_type]

                    primary_key = annotations['primaryKey']
                    sequence = annotations['sequence']

                    if primary_key and sequence:
                        sequence_cache = int(annotations['sequenceCache']) if annotations['sequenceCache'] else None
                        column_args.append(Sequence(sequence, cache=sequence_cache))

                    column = Column(*column_args,
                                    primary_key=primary_key,
                                    nullable=annotations['nullable'],
                                    server_default=annotations['default'],
                                    unique=annotations['unique'],
                                    index=annotations['index'],)

                    columns.append(column)

                table = Table(entity.name, metadata, *columns, schema=schema)
                log.debug(f'Скрипт создания таблицы {entity.name}: {CreateTable(table).compile(metadata.bind)}')
                table.create()
                log.info(f'Таблица {schema}.{table.name} подготовлена.')
        else:
            table = None
            log.debug(f'Сущность {entity.name} не является локальной, обработка не требуется.')
        return table

    @staticmethod
    def create_relationship(cdm_relationship, engine=None, metadata=None, schema=None, entities_schemas=None):
        """ Добавляет связь между таблицами (foreign keys) на основе CDM relationship объектов. """

        if cdm_relationship.get_note('generateKey'):
            log.info(f'Обрабатывается связь {cdm_relationship}.')

            sql_tmpl = """
            alter table {from_e_schema}{from_e}
    add constraint {fk_name} foreign key({from_a})
    references {to_e_schema}{to_e}({to_a}){on_update}{on_delete}
            """

            schema_str = f'{schema}.' if schema else ''
            metadata = get_engine_metadata(engine=engine, metadata=metadata, schema=schema)
            dialect = metadata.bind.dialect.name

            from_entity = cdm_relationship.fromAttribute.entityName
            to_entity = cdm_relationship.toAttribute.entityName

            if entities_schemas:
                from_e_schema = entities_schemas.get(from_entity)
                to_e_schema = entities_schemas.get(to_entity)
            else:
                from_e_schema = to_e_schema = schema
            from_e_schema_str = f'{from_e_schema}.' if from_e_schema else schema_str
            to_e_schema_str = f'{to_e_schema}.' if to_e_schema else schema_str

            metadata.reflect(only=[from_entity, ], schema=from_e_schema)
            metadata.reflect(only=[to_entity, ], schema=to_e_schema)

            __ = Table(from_entity, metadata, autoload=True, schema=from_e_schema)  # для ссылки из to_
            from_attribute = cdm_relationship.fromAttribute.attributeName
            to_attribute = cdm_relationship.toAttribute.attributeName

            foreign_keys = (e.target_fullname.lower() for e in
                            metadata.tables[f'{from_e_schema_str}{from_entity}'].foreign_keys)

            if f'{to_entity}.{to_attribute}'.lower() not in foreign_keys:
                name = getattr(cdm_relationship, 'name')
                fk_name = name or f'fk_{from_entity}_{from_attribute}'
                on_update = cdm_relationship.get_note('onUpdate')
                on_delete = cdm_relationship.get_note('onDelete')
                on_update = f'\non update {on_update}' if on_update else ''
                on_delete = f'\non delete {on_delete}' if on_delete else ''

                if dialect not in ('mssql', 'postgresql', 'qhb'):
                    raise DFOperatorException(f'Создание внешних ключей для <{dialect}> не поддерживается')

                sql = sql_tmpl.format(from_e_schema=from_e_schema_str, to_e_schema=to_e_schema_str,
                                      fk_name=fk_name.lower(), from_e=from_entity, to_e=to_entity, from_a=from_attribute,
                                      to_a=to_attribute, on_update=on_update, on_delete=on_delete)
                try:
                    execute_sql(sql, engine=engine)
                except psycopg2.errors.DuplicateObject:  # @UndefinedVariable
                    log.debug(f'Связь {cdm_relationship} уже существует.')

            log.info(f'Связь {cdm_relationship} подготовлена.')


def op_execute(q):
    log.debug(f'Выполняется запрос:\n{q}')
    with op.get_context().autocommit_block():  # @UndefinedVariable
        try:
            op.execute(q)  # @UndefinedVariable
        except ProgrammingError:
            pass


def drop_db_obj(obj, name, schema='', cascade=True):
    schema = get_clean_schema(schema, env_var=False, dotted=True)
    cascade = 'cascade' if cascade else ''
    q = f"drop {obj} if exists {schema}{name} {cascade};"
    op_execute(q)


def drop_table(name, schema='', cascade=True):
    drop_db_obj('table', name, schema=schema, cascade=cascade)


def drop_type(name, schema='', cascade=False):
    drop_db_obj('type', name, schema=schema, cascade=cascade)


def drop_function(name, schema=''):
    schema = get_clean_schema(schema, env_var=False, dotted=True)
    q = f"if object_id('{schema}{name}', 'FN') is not null drop function {schema}{name};"
    op_execute(q)
