"""
Created on Mar 25, 2020

@author: pymancer@gmail.com
"""
import requests

from requests.compat import urljoin
from requests_auth import NTLM, Basic
from df.common.helpers.logger import Logger
from df.common.helpers.general import get_decoded_connection, sort_dataset_values  # @UnresolvedImport
from ..reader import AbstractReader, AbstractSheet

log = Logger()


class JSONMapSheet(AbstractSheet):
    """ "Лист" данных в виде списка JSON словарей. """
    def to_array(self):
        array = list()

        if self.data:
            # есть как минимум одна запись, создаем из нее строку с наименованиями колонок
            columns_row = list()

            for column, value in self.data[0].items():

                if type(value) in (list, dict):
                    log.warning(f'Вложенная структура [{column}] пропущена')
                    continue

                columns_row.append(column)

            array.append(columns_row)
            values = [sort_dataset_values(vd, columns_row) for vd in self.data]
            array.extend(values)

        return array


class Reader(AbstractReader):
    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        """ Возвращает "поток" из источника в виде списка с одним "листом" данных. """
        conn = get_decoded_connection(postfix='PRODUCER')
        url = urljoin(conn['host'], path)
        extra = conn.get('extra', dict())
        auth_type = extra.get('auth', 'basic').lower()

        log.debug(f'Запрашиваем данные с {url}')

        if auth_type == 'ntlm':
            domain = extra.get('domain')
            full_login = f"{domain}\\{conn['login']}" if domain else conn['login']

            data = requests.get(url, auth=NTLM(full_login, conn['pass']))
        else:
            data = requests.get(url, auth=Basic(conn['login'], conn['pass']))

        sheet = JSONMapSheet(data.json())

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream
        return self.sheet_stream[sheet].to_array()
