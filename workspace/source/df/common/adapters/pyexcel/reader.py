"""
Created on Mar 25, 2020

@author: pymancer@gmail.com
"""
import pyexcel
import openpyxl

from ..reader import AbstractReader
from df.common.exceptions import DFAdapterException  # @UnresolvedImport


class Reader(AbstractReader):
    """ Ридер файлов Excel, OpenOffice, CSV. """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.flavor = kwargs.get('flavor')

    @property
    def sheets(self):
        return self.stream.sheets

    def get_stream(self, path, **kwargs):
        if self.flavor == 'pyexcel-csvr':
            self.stream = pyexcel.iget_book(file_name=path,
                                            encoding=kwargs['encoding'],
                                            delimiter=kwargs['delimiter'],
                                            auto_detect_float=kwargs['auto_detect_float'],
                                            auto_detect_int=kwargs['auto_detect_int'],
                                            auto_detect_datetime=kwargs['auto_detect_datetime'])
        elif self.flavor == 'pyexcel-htmlr':
            ext = path.split('.')[-1]
            content = kwargs.get('content')

            if ext != 'html' and not content:  # загрузка html-таблицы с другим разрешением не проходит
                content = self._get_content(path)

            if content:
                self.stream = pyexcel.iget_book(file_type="html", file_content=content)
            else:
                self.stream = pyexcel.iget_book(file_name=path, library='pyexcel-htmlr')
        else:
            ext = path.split('.')[-1]

            if ext in ('xlsx', 'xlsm'):
                # ручной выбор библиотеки есть только для .xlsx(m), да и то не всегда
                library = 'pyexcel-xlsx' if self.flavor == 'openpyxl' else self.flavor
            else:
                library = None

            if ext in ('xls', 'xlsx', 'xlsm'):
                self.stream = pyexcel.iget_book(file_name=path, library=library,
                                                skip_hidden_sheets=kwargs['skip_hidden_sheets'],
                                                skip_hidden_row_and_column=kwargs['skip_hidden_row_and_column'],
                                                detect_merged_cells=kwargs['detect_merged_cells'])
            else:
                self.stream = pyexcel.iget_book(file_name=path, library=library)

        return self.stream

    @staticmethod
    def _get_content(path):
        with open(path, "rb") as f:
            content = f.read()
            return content

    def get_rows(self, **kwargs):
        sheet_index = kwargs.get('sheet', 0)
        sheet = self.stream[sheet_index]

        if not sheet:
            raise DFAdapterException(f'Лист #{sheet_index} не найден')

        sheet_name = sheet.name
        for idx, name in enumerate(self.sheets):
            if sheet_name == name:
                sheet_index = idx
                break
        else:
            raise DFAdapterException(f'Лист {sheet} не найден')

        if self.flavor == 'openpyxl':
            read_only = kwargs.get('read_only', False)
            filename = kwargs['filepath']
            wb = openpyxl.load_workbook(filename=filename, read_only=read_only)
            wb.active = sheet_index
            self.sheet_stream = wb.active
            rows = self.sheet_stream.rows
        else:
            self.sheet_stream = self.stream[sheet_index]
            rows = pyexcel.iget_array(sheet_stream=self.sheet_stream, library=self.flavor)

        return rows

    def free_resources(self):
        super().free_resources()
        pyexcel.free_resources()
