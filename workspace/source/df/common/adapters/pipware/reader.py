"""
Created on Mar 25, 2020

@author: pymancer@gmail.com
"""
import requests

from requests.compat import urljoin
from df.common.helpers.logger import Logger
from df.common.helpers.general import get_decoded_connection  # @UnresolvedImport
from df.common.exceptions import DFPIPwareHookException  # @UnresolvedImport
from ..reader import AbstractReader, AbstractSheet

log = Logger()


class Hook:
    """ Взаимодействует с API PIPware. """
    def __init__(self, url, token, api_version=None, extra=None):
        self.url = url
        self.token = token
        self.api_version = api_version
        self.extra = extra

    def _parse_response(self, response):
        content_type = response.headers.get('Content-Type')
        if content_type and response.text and 'application/json' in content_type:
            result = response.json()
        else:
            result = response.text
        return result

    def call(self, resource='', method='get', extra_headers=None, data=None, params=None):
        """ Универсальный вызов API.
            Args:
                :resource - адрес ресурса относительно API entry point (Connection.host)
                :method - тип сообщения, обычно get или post
                :extra_headers - http заголовки, дополняющие, либо переопределяющие стандартные
                :data - полезная нагрузка запроса, как правило - словарь
                :params - параметры url запроса, как правило - словарь
            Returns:
                Ответ сервера в виде словаря
        """
        headers = {'X-PIPWARE-ApiKey': self.token}
        if extra_headers:
            headers.update(extra_headers)

        url = urljoin(self.url, resource)

        try:
            r = requests.request(method, url, headers=headers, json=data, params=params)
        except Exception as e:
            headers['Authorization'] = '*******'
            raise DFPIPwareHookException(f'{method.upper()} [{url}], хидеры:\n{headers}\nошибка:\n{e}')

        answer = self._parse_response(r)

        if r.status_code != requests.codes.ok:  # @UndefinedVariable
            answer = self._parse_response(r)
            msg = f'{method.upper()} [{url}], код ошибки: {r.status_code}, ответ:\n<--\n{answer}\n-->'
            raise DFPIPwareHookException(msg)

        return self._parse_response(r)


class PIPwareSheet(AbstractSheet):
    """ "Лист" данных из PIPware API. """
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

            for row in self.data:
                # создаем строки данных
                record = list()

                for column in columns_row:
                    record.append(row[column])

                array.append(record)

        return array


class Reader(AbstractReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.options = kwargs.get('options')

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        """ Возвращает "поток" из источника в виде списка с одним "листом" данных. """
        params=None
        conn = get_decoded_connection(postfix='PRODUCER')
        hook = Hook(url=conn['host'], token=conn['extra'].get('apiKey', ''))

        if self.options:
            params = dict()

            for option in self.options:
                value = option.value

                if value == 'True':
                    value = 'true'
                elif value == 'False':
                    value = 'false'
                elif value == 'None':
                    value = 'null'

                if option.name in params:
                    # опция содержит несколько значений
                    if isinstance(params[option.name], list):
                        params[option.name].append(value)
                    else:
                        params[option.name] = [params[option.name], value]
                else:
                    params[option.name] = value

        sheet = PIPwareSheet(hook.call(resource=path, params=params))

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream
        return self.sheet_stream[sheet].to_array()
