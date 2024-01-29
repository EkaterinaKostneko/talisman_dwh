"""
Created on Jan 13, 2021

@author: pymancer@gmail.com
"""
from df.common.schemas.model import DataType # @UnresolvedImport
from df.common.exceptions import DFUploaderException, DFViHookException  # @UnresolvedImport
from df.common.helpers.logger import Logger
from df.common.helpers.general import Logger, get_db_schema, get_conn_parameters, sort_dataset_values  # @UnresolvedImport
from df.common.helpers.sql import get_attribute_annotations  # @UnresolvedImport
from df.common.helpers.viapi import ViHook  # @UnresolvedImport
from ..uploader import AbstractColumn, AbstractContainer, AbstractUploader

log = Logger()


class Column(AbstractColumn):
    def __init__(self, name, primary=False, sequence=None, **kwargs):
        super().__init__(name, **kwargs)
        self.primary = primary
        self.sequence = sequence

    def __eq__(self, other):
        return self.name == other.name if isinstance(other, Column) else self.name == other


class Container(AbstractContainer):
    ...


class ViqubeUploader(AbstractUploader):
    def __init__(self, entity, batch_size=10000, **kwargs):
        super().__init__(entity, **kwargs)
        self._container = None
        self.batch = batch_size

        self._conn_parameters = get_conn_parameters()
        self._extra = self._conn_parameters.get('extra', dict())
        self.schema = entity.get_note('schema') or get_db_schema() or self._extra.get('schema', 'DB')

    @property
    def conn(self):
        if not self._conn:
            self._conn = ViHook(self._conn_parameters['login'],
                                self._conn_parameters['pass'],
                                self._conn_parameters['host'],
                                api_version=self._extra.get('api_version'),
                                extra=self._extra)

        return self._conn

    def upload(self, dataset, entity=None, container=None, **kwargs):
        if not container:
            container = self.get_container(entity=entity)

        tables_resource = self._get_resource_tables()
        resource = f'{tables_resource}/{container.name}/records'

        columns = [c.name for c in container.columns if not c.sequence]
        values = [sort_dataset_values(vd, columns) for vd in dataset]
        data = {'columns': columns, 'values': values}

        self.conn.call(resource=resource, method='post', data=data)

    def create(self, entity=None, **kwargs):
        entity = entity or self.entity
        resource = self._get_resource_tables()
        data = {'name': entity.name, 'columns': list()}

        for attribute in entity.attributes:
            # по умолчанию тип атрибута всегда строковый
            type_ = attribute.dataType if attribute.dataType != DataType.Unclassified else DataType.String
            column = {'name': attribute.name, 'type': type_.toJson().lower()}

            annotations = get_attribute_annotations(attribute)
            primary_key = annotations['primaryKey']
            sequence = annotations['sequence']
            role = annotations['role']

            if 'primary' in data and (role == 'id' or primary_key):
                DFUploaderException(f"Попытка перезаписи первичного ключа с {data['primary']} на {attribute.name}")
            else:
                if role == 'id':
                    data['primary'] = attribute.name
                    sequence_name = self.get_sequence_name(entity=entity, attr='id')
                    self.create_sequence(name=sequence_name)
                    column['sequence'] = sequence_name
                    column['type'] = DataType.Long.toJson()
    
                if primary_key:
                    data['primary'] = attribute.name
    
                    if sequence and type_ in (DataType.Integer, DataType.Long, DataType.Short):
                        self.create_sequence(name=sequence)
                        column['sequence'] = sequence

            data['columns'].append(column)

        try:
            self.conn.call(resource=resource, method='post', data=data)
        except DFViHookException as e:
            code = self.conn.get_error_code(e)
            if code == 409:
                log.debug(f'Создаваемая таблица {entity.name} уже существует')
            else:
                raise e

        return self.get_container(entity=entity)

    def drop(self, entity=None, **kwargs):
        resource = self._get_resource_table(entity=entity)
        try:
            self.conn.call(resource=resource, method='delete')
        except DFViHookException as e:
            code = self.conn.get_error_code(e)
            if code == 404:
                log.debug(f'Удаляемая таблица {entity.name} не найдена')
            else:
                raise e

        self.drop_sequences(entity=entity)

        if not entity:
            self._container = None

    def empty(self, entity=None, **kwargs):
        table_resource = self._get_resource_table(entity=entity)
        resource = f'{table_resource}/records/all'
        self.conn.call(resource=resource, method='delete')

    def get_container(self, entity=None, **kwargs):
        if not self._container:
            table = self._get_table(entity=entity)

            if table:
                columns = list()
                for c in table['columns']:
                    column = Column(name=c['name'])

                    if 'sequence' in c:
                        column.sequence = c['sequence']

                    if 'primary' in table and c['name'] == table['primary']:
                        column.primary = True

                    columns.append(column)

                self._container = Container(name=table['name'], columns=columns)

        return self._container

    def _get_table(self, entity=None):
        table = None
        resource = self._get_resource_table(entity=entity)

        try:
            table = self.conn.call(resource=resource, method='get')
        except DFViHookException as e:
            code = self.conn.get_error_code(e)
            if code == 404:
                log.debug(f'Таблица {entity.name} не найдена')
            else:
                raise e

        return table

    def _get_resource_db(self):
        return f'viqube/databases/{self.schema}'

    def _get_resource_tables(self):
        resource = self._get_resource_db()
        return f'{resource}/tables'

    def _get_resource_table(self, entity=None):
        entity = entity or self.entity
        resource = self._get_resource_tables()
        return f'{resource}/{entity.name}'

    def _get_resource_sequences(self):
        resource = self._get_resource_db()
        return f'{resource}/sequences'

    def get_sequence_name(self, entity=None, attr=None, name=None):
        if not name:
            entity = entity or self.entity
            attr_str = f'_{attr}' if attr else ''
            name = f'{entity.name}{attr_str}_sq'

        return name

    def _get_resource_sequence(self, entity=None, attr=None, name=None):
        name = self.get_sequence_name(entity=entity, attr=attr)
        resource = self._get_resource_sequences()
        return f'{resource}/{name}'

    def create_sequence(self, entity=None, attr=None, name=None):
        name = self.get_sequence_name(entity=entity, attr=attr, name=name)
        resource = self._get_resource_sequences()
        data = {'name': name}

        try:
            self.conn.call(resource=resource, method='post', data=data)
        except DFViHookException as e:
            code = self.conn.get_error_code(e)
            if code == 409:
                log.debug(f'Создаваемый sequence {name} не найден')
            else:
                raise e

    def drop_sequence(self, entity=None, attr=None, name=None):
        name = self.get_sequence_name(entity=entity, attr=attr, name=name)
        resource_sequences = self._get_resource_sequences()
        resource = f'{resource_sequences}/{name}'

        try:
            self.conn.call(resource=resource, method='delete')
        except DFViHookException as e:
            code = self.conn.get_error_code(e)
            if code == 404:
                log.debug(f'Удаляемый sequence {name} не найден')
            else:
                raise e

    def drop_sequences(self, entity=None):
        table = self._get_table(entity=entity)

        if table:
            for column in table['columns']:
                if 'sequence' in column:
                    self.drop_sequence(name=column['sequence'])
