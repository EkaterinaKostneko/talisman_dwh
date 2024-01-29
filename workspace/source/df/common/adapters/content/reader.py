"""
Created on Dec 7, 2022

@author: vmikhaylov@polyanalitika.ru
"""
import os
import json
from pathlib import PosixPath

from ..reader import AbstractReader, AbstractSheet
from df.common.exceptions import DFAdapterException  # @UnresolvedImport
from df.common.helpers.sql import Op  # @UnresolvedImport
from df.common.helpers.logger import Logger

log = Logger()


class ContentSheet(AbstractSheet):
    """ "Лист" данных из файла """

    def __init__(self, *args, path=None, **kwargs):
        super(ContentSheet, self).__init__(*args, **kwargs)

        self.expiration = None
        self.meta = None
        if path:
            try:
                file_meta = self.stat_to_json(path)
                file_meta['path'] = str(PosixPath(path))
                file_meta['name'] = os.path.basename(PosixPath(path))
                self.meta = json.dumps(file_meta)
            except TypeError as e:
                log.error(f'Ошибка получения meta для файла <{path}>:\n{e}')

    @staticmethod
    def stat_to_json(fp: str) -> dict:
        s_obj = os.stat(fp)
        return {k: getattr(s_obj, k) for k in dir(s_obj) if k.startswith('st_')}

    def to_array(self) -> list:
        array = list()
        # есть как минимум одна запись, создаем из нее строку с наименованиями колонок
        if self.data:
            # возвращаем набор достаточный для вставки: op_ins_id - идентификатор таска, загружающего данные;
            # т.к. адаптер текстовый, то содержимое файла кладем в поле value
            columns = ('op_ins_id', 'expiration', 'meta', 'value')
            data = (Op().ins_id, self.expiration, self.meta, self.data)
            array.append(columns)
            array.append(data)
        return array


class Reader(AbstractReader):
    """ Ридер содержимого различных файлов """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        data = self._get_content(path)
        sheet = ContentSheet(data, path=path)
        self.stream = [sheet]
        return self.stream

    @staticmethod
    def _get_content(path):
        with open(path, "r") as f:  # нужны варианты для больших файлов...
            content = f.read()
            return content

    def get_rows(self, **kwargs):
        sheet_index = kwargs.get('sheet', 0)
        sheet = self.stream[sheet_index]

        if not sheet:
            raise DFAdapterException(f'Лист #{sheet_index} не найден')

        self.sheet_stream = sheet
        rows = self.sheet_stream.to_array()

        return rows

    def get_adapted_rows(self, **kwargs):
        rows = self.get_rows()
        columns = rows[0]
        data = rows[1:]
        return super(Reader, self).get_adapted_rows(rows=data, columns=columns)

    def free_resources(self):
        super().free_resources()
