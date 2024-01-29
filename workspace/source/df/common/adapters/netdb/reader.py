"""
Created on Mar 25, 2020

@author: pymancer@gmail.com
"""
import pyexcel

from xml.etree import ElementTree as ET
from ..reader import AbstractReader


class Reader(AbstractReader):
    """ Ридер XML файлов в формате NetDB.
        Если бы не params можно было бы использовать pyexcel-htmlr нативно.
    """
    @property
    def sheets(self):
        if not self._sheets:
            for sheet in range(self.stream.number_of_sheets()):
                self._sheets[sheet] = self.stream.sheet_by_index(sheet)

        return self._sheets

    def set_parameters(self, filepath):
        """ Итеративно считывает и запоминает параметры датасета
            В соответствии с форматом XML API NetDB (v3.12) подразумеваются следующие ограничения:
              - в одном файле только один элемент `<table></table'
              - в каждом файле присутствует элемент '<params></params>' даже если самих параметров нет
        """
        parameters = dict()

        for idx, i in enumerate(ET.iterparse(filepath), start=1):
            __, el = i
            if el.tag == 'param':
                parameters[f'@ndbparam{idx}'] = el.text
            elif el.tag =='params':
                break

        self.parameters = parameters

    def get_stream(self, path, **kwargs):
        self.set_parameters(path)

        self.stream = pyexcel.get_book(file_name=path,
                                       force_file_type='html',
                                       spread_merged_rows=kwargs['spread_merged_rows'],
                                       spread_merged_columns=kwargs['spread_merged_columns'])

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream

        return self.sheet_stream[sheet].to_array()
