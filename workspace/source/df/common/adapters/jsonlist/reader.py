import requests

from urllib.parse import urljoin
from requests_auth import NTLM, Basic
from df.common.helpers.logger import Logger
from df.common.helpers.general import get_decoded_connection  # @UnresolvedImport
from ..reader import AbstractReader

log = Logger()


class Reader(AbstractReader):
    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        """ Получает "Лист" данных в виде словаря JSON массивов.
        Возвращает "поток" из источника в виде списка с одним "листом" данных. """
        conn = get_decoded_connection(postfix='PRODUCER')
        url = urljoin(conn['host'], path)
        extra = conn.get('extra', dict())
        auth_type = extra.get('auth', 'basic').lower()
        columns_name = extra.get('columns_name', 'columns').lower()
        data_name = extra.get('data_name', 'data').lower()

        log.debug(f'Запрашиваем данные с {url}')

        if auth_type == 'ntlm':
            domain = extra.get('domain')
            full_login = f"{domain}\\{conn['login']}" if domain else conn['login']

            data = requests.get(url, auth=NTLM(full_login, conn['pass'])).json()
        else:
            data = requests.get(url, auth=Basic(conn['login'], conn['pass'])).json()

        sheet = [data[columns_name], *data[data_name]]

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream
        return self.sheet_stream[sheet]
