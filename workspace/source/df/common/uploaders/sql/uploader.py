"""
Created on Dec 17, 2020

@author: pymancer@gmail.com
"""
from sqlalchemy import create_engine, MetaData
from df.common.helpers.logger import Logger
from df.common.helpers.sql import get_formatting_table, truncate_table, MetadataWarlock  # @UnresolvedImport
from df.common.helpers.general import (sort_dataset_values,  # @UnresolvedImport
                                       get_conn_string, get_db_schema, get_db_dialect)  # @UnresolvedImport
from ..uploader import AbstractUploader

log = Logger()


class SQLUploader(AbstractUploader):
    def __init__(self, entity, batch_size=10000, conn_string=None, schema=None, **kwargs):
        super().__init__(entity, **kwargs)
        conn_string = conn_string or get_conn_string()
        self.schema = entity.get_note('schema') or schema or get_db_schema()
        self.bulk = self.entity.get_note('useBulkInsert', default=True)
        self.batch = batch_size
        # пытаемся включить режим быстрой пакетной вставки в таблицы БД
        dialect = get_db_dialect()
        engine_kwargs = dict()

        if self.bulk:
            if dialect == 'mssql':
                engine_kwargs['fast_executemany'] = True
            elif dialect in ('postgresql', 'qhb'):
                engine_kwargs['executemany_mode'] = 'values'
                engine_kwargs['executemany_values_page_size']=self.batch
                engine_kwargs['executemany_batch_page_size']=self.batch

        self.engine = create_engine(conn_string, **engine_kwargs)
        self.metadata = MetaData(bind=self.engine, schema=self.schema)

    @property
    def conn(self):
        if not self._conn:
            log.debug(f'Попытка соединения с сервером {self.engine.name}...')
            if self.bulk:
                self._conn = self.engine.pool._creator()
            else:
                self._conn = self.engine.connect()
            log.debug(f'Соединение с сервером {self.engine.name} установлено.')

        return self._conn

    def upload(self, dataset, entity=None, container=None, **kwargs):
        if container is None:
            container = self.get_container(entity=entity)

        if self.bulk:
            cursor = self.conn.cursor()
            # оставляем только те атрибуты, которым можем передать значения
            compiled = (container.insert().compile(dialect=self.engine.dialect, column_keys=dataset[0].keys()))

            if compiled.positional:
                # получаем порядок атрибутов для вставки
                ordered_keys = list(compiled.params.keys())
                # приводим порядок значений для вставки к порядку следования атрибутов
                dataset = [sort_dataset_values(vd, ordered_keys) for vd in dataset]
            # выполняем быструю вставку датасета
            cursor.executemany(str(compiled), dataset)
            cursor.close()
        else:
            self.conn.execute(container.insert(), dataset)

    def create(self, entity=None, rid_attr=None, **kwargs):
        name = self.get_container_name(entity=entity)

        if rid_attr:
            return get_formatting_table(name, rid_attr, self.metadata, self.schema)
        else:
            entity = self.entity or entity
            MetadataWarlock.create_entity(entity, metadata=self.metadata, schema=self.schema)
            self.metadata.reflect(only=(name,), schema=self.schema)
            return self.get_container(entity=entity)

    def drop(self, entity=None, **kwargs):
        full_name = self.get_container_name(entity=entity, full=True)
        table = self.metadata.tables.get(full_name, None)
        table.drop(self.engine)
        self.metadata.remove(table)

    def empty(self, entity=None, **kwargs):
        name = self.get_container_name(entity=entity)
        truncate_table(name, schema=self.schema, engine=self.engine)

    def dispose(self, **kwargs):
        self.engine.dispose()

    def get_container(self, entity=None, **kwargs):
        full_name = self.get_container_name(entity=entity, full=True)
        table = self.metadata.tables.get(full_name, None)

        if table is None:
            name = self.get_container_name(entity=entity)
            self.metadata.reflect(only=(name,), schema=self.schema)
            table = self.metadata.tables.get(full_name, None)

        return table

    def get_container_name(self, entity=None, full=False, **kwargs):
        entity = entity or self.entity 
        name = entity.name

        if full and self.schema:
            name = f'{self.schema}.{name}'

        return name

    def commit(self, **kwargs):
        if self.bulk:
            self.conn.commit()
