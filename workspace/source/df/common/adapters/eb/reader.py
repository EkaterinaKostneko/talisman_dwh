from xml.etree import ElementTree as ET
from ..reader import AbstractReader, AbstractSheet


class EBSheet(AbstractSheet):
    """ "Лист" данных из файла электронного бюджета """

    def to_array(self):
        array = list()

        # есть как минимум одна запись, создаем из нее строку с наименованиями колонок
        if len(self.data) > 0:
            columns_row = list()
            for field in self.data[0]:
                columns_row.append(field.tag)

            array.append(columns_row)

            for record in self.data:
                # создаем строки данных
                row = list()
                for field in record:
                    row.append(field.text)
                array.append(row)

        return array


class Reader(AbstractReader):
    """ Ридер XML файлов в формате Электронного бюджета """

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        tree = ET.parse(path)
        root = tree.getroot()

        sheet = EBSheet(root)

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream

        return self.sheet_stream[sheet].to_array()
