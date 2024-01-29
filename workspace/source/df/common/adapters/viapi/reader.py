"""
Created on Jul 29, 2020

@author: pymancer@gmail.com
"""
from df.common.exceptions import DFViAPIException  # @UnresolvedImport
from df.common.helpers.logger import Logger
from df.common.helpers.general import get_decoded_connection  # @UnresolvedImport
from df.common.helpers.viapi import ViHook  # @UnresolvedImport
from ..reader import AbstractReader, AbstractSheet

log = Logger()


class DCDimensionSheet(AbstractSheet):
    """ "Лист" данных из DC API. """
    def __init__(self, data, **kwargs):
        super().__init__(data, **kwargs)
        self.delimiter = kwargs.get('delimiter', '/')

    def to_array(self):
        array = list()

        for batch in self.data:
            if not array:
                # первый набор данных, создаем строку с наименованиями и id колонок
                try:
                    attrs = batch['dimension']['attributes']
                except KeyError:
                    raise DFViAPIException(f'Данные не содержат информации об атрибутах:\n{batch}')
    
                columns_row = ['id', 'name', 'path']
    
                columns_indexes = {-3: 0, -2: 1, -1: 2}
    
                for idx, attr in enumerate(sorted(attrs, key=lambda k: k['id']), start=3):
                    columns_row.append(attr['name'])
                    # ordinal атрибутов определяет порядок их id
                    columns_indexes[attr['id']] = idx
    
                array.append(columns_row)

            for row in batch.get('elements', list()):
                # создаем строки данных
                path = self.delimiter.join([i['folderName'] for i in row['path']])
                record = [row['id'], row['name'], path]

                for i in range(3, len(columns_row)):
                    record.insert(i, None)

                for attr in row.get('attributes', list()):
                    # добавляем атрибуты в порядке возрастания их id, сразу после id, name и path
                    record[columns_indexes[attr['attributeId']]] = attr['value']

                array.append(record)

        return array


class Reader(AbstractReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        """ Возвращает "поток" из источника в виде списка с одним "листом" данных. """
        data = list()
        conn = get_decoded_connection(postfix='PRODUCER')
        hook = ViHook(conn['login'], conn['pass'], conn['host'], extra=conn['extra'])

        # TODO: получать данные асинхронно пачками размером указанным в extra['batch'] или в метаданных `dcBatchSize`
        # здесь path должен проверяться регуляркой, по результату должен выбираться конкретные Sheet и функция загрузки
        batch = None
        try:
            batch = int(kwargs.get('dc_batch_size') or conn['extra']['batch'])
        except (KeyError, TypeError):
            pass

        if batch:
            current = 0
            while True:
                log.debug(f'Получение данных через API Visiology, запрос {current}')
                query = {'skip': batch * current, 'limit': batch} 
                answer = hook.call(resource=path, data=query)

                # TODO: проверять наличие данных должна соответствующая, осведомленная, функция загрузки
                if answer.get('elements'):
                    data.append(answer)
                    current += 1
                else:
                    break
        else:
            log.debug(f'Получение полного набора данных через API Visiology')
            data.append(hook.call(resource=path, data={'getAll': True}))

        sheet = DCDimensionSheet(data, delimiter=kwargs['path_delimiter'])

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream
        return self.sheet_stream[sheet].to_array()