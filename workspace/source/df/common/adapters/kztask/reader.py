import requests

from requests.compat import urljoin
from df.common.helpers.logger import Logger
from df.common.helpers.general import get_decoded_connection  # @UnresolvedImport
from df.common.helpers.json import denormalize, flatten_record
from df.common.exceptions import DFKZTaskHookException
from ..reader import AbstractReader, AbstractSheet

log = Logger()


class Hook:
    """ Взаимодействует с API Поручений """

    def __init__(self, url, login, password, auth_endpoint="login"):
        """:param url: адрес хоста (Connection.host)
        :param auth_endpoint: эндпоинт авторизации (из Connection.extra)
        :param login: логин (имя пользователя) для авторизации
        :param password: пароль для авторизации"""
        self.url = url
        self.login = login
        self.password = password
        self.auth_endpoint = auth_endpoint
        self._token = None

    @property
    def token(self):
        if not self._token:
            data = {'username': self.login, 'password': self.password}
            url = urljoin(
                self.url, self.auth_endpoint)
            try:
                res = requests.post(url=url, json=data)
            except Exception as e:
                raise DFKZTaskHookException(f"Ошибка при авторизации: {e}")

            if res.status_code != requests.codes.ok:
                answer = self._parse_response(res)
                msg = f'POST [{url}], код ошибки: {res.status_code}, ответ:\n<--\n{answer}\n-->'
                raise DFKZTaskHookException(msg)

            self._token = str(res.headers.get('Authorization'))

        return self._token

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
        headers = {'Authorization': f"Bearer {self.token}"}
        if extra_headers:
            headers.update(extra_headers)

        url = urljoin(self.url, resource)

        try:
            r = requests.request(
                method, url, headers=headers, json=data, params=params,)
        except Exception as e:
            headers['Authorization'] = '*******'
            raise DFKZTaskHookException(
                f'{method.upper()} [{url}], хидеры:\n{headers}\nошибка:\n{e}')

        answer = self._parse_response(r)

        if r.status_code != requests.codes.ok:
            answer = self._parse_response(r)
            msg = f'{method.upper()} [{url}], код ошибки: {r.status_code}, ответ:\n<--\n{answer}\n-->'
            raise DFKZTaskHookException(msg)

        return self._parse_response(r)


class KZTaskSheet(AbstractSheet):
    """ Лист данных Поручений """

    def to_array(self):
        array = list()

        if len(self.data) > 0:
            columns_row = list()
            denormalized_table = list()
            flat_table = list()

            # Делаем плоскую таблицу
            for row in self.data:
                for subrow in denormalize(row):
                    if subrow not in denormalized_table:
                        denormalized_table.append(subrow)
            for row in denormalized_table:
                flat_table.append(flatten_record(row, '.'))

            # Получаем заголовки
            for row in flat_table:
                for column, value in row.items():
                    if column not in columns_row:
                        columns_row.append(column)

            array.append(columns_row)

            # Собираем строчки
            for row in flat_table:
                record = list()
                for column in columns_row:
                    if column in row.keys():
                        record.append(row[column])
                    else:
                        record.append(None)

                array.append(record)

        return array


class Reader(AbstractReader):
    """ Ридер ответов от API Поручений """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.options = kwargs.get("options")

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def get_stream(self, path, **kwargs):
        """ Возвращает "поток" из источника в виде списка с одним "листом" данных. """
        params = None
        conn = get_decoded_connection(postfix='PRODUCER')
        hook = Hook(url=conn['host'], login=conn['login'], password=conn['pass'],
                    auth_endpoint=conn['extra'].get('auth_endpoint'))

        if self.options:
            params = dict()

            for option in self.options:
                value = option.value

                if option.name in params:
                    if isinstance(params[option.name], list):
                        params[option.name].append(value)
                    else:
                        params[option.name] = [params[option.name], value]
                else:
                    params[option.name] = value

        sheet = KZTaskSheet(hook.call(resource=path, params=params))

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream
        return self.sheet_stream[sheet].to_array()
