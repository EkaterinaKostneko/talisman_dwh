"""
Created on Jun 17, 2020

@author: pymancer@gmail.com
"""
import re
import requests

from time import sleep, perf_counter
from requests.compat import urljoin
from df.common.constants import DC_API_VERSION  # @UnresolvedImport
from df.common.exceptions import DFViHookException  # @UnresolvedImport


class ViHook:
    """ Взаимодействует с API сервиса Visiology. """
    def __init__(self, user, password, url, api_version=None, extra=None):
        """ Инициализация параметров подключения к викубу """
        self._context = {'user': user,
                         'pass':  password,
                         'url':  url,
                         'api':  api_version or extra.get('api_version')}
        # для внешнего доступа
        self.extra = extra or dict()

        self.threshold = 0.05
        self.called = perf_counter() - self.threshold

        self._conn = self._get_conn()

        self._error_code_pattern = re.compile(r'код ошибки: (\d{3}),')

    def _parse_response(self, response):
        content_type = response.headers.get('Content-Type')
        if content_type and response.text and 'application/json' in content_type:
            result = response.json()
        else:
            result = response.text
        return result

    def _get_conn(self):
        ctx = self._context
        conn = dict()
        unit = self.extra.get('unit')
        if unit == 'viqube':
            scope = self.extra.get('scope', 'viqube_api viqubeadmin_api')
            scope_name = 'scope'
            authorization = 'Basic dmlxdWJlYWRtaW5fcm9fY2xpZW50OjcmZEo1UldwVVMkLUVVQE1reHU='
        elif unit == 'dc':
            scope = self.extra.get('scopes', 'viewer_api core_logic_facade')
            scope_name = 'scopes'
            authorization = 'Basic cm8uY2xpZW50OmFtV25Cc3B9dipvfTYkSQ=='
        else:
            raise DFViHookException(f'Подсистема <{unit}> API Visiology не поддерживается')

        grant_type = self.extra.get('grant_type', 'password')
        response_type = self.extra.get('response_type', 'id_token token')
        conn['entry_point'] = ctx['url']
        conn['verify'] = self.extra.get('ssl_verify', True)

        if ctx['api']:
            conn['api_version'] = ctx['api']
        else:
            if unit == 'viqube':
                # запрашиваем версию API у целевого сервиса
                url = urljoin(conn['entry_point'], 'viqube/version')

                try:
                    r = requests.get(url=url, verify=conn['verify'])
                except Exception as e:
                    raise DFViHookException(f'Ошибка получения версии API:\n{e}')

                if r.status_code == requests.codes.ok:  # @UndefinedVariable
                    conn['api_version'] = r.json()['apiStable']
                else:
                    answer = self._parse_response(r)
                    msg = f'Http ошибка при полученнии версии API: {r.status_code}, ответ:\n{answer}'
                    raise DFViHookException(msg)
            else:
                conn['api_version'] = DC_API_VERSION

        params = {'grant_type': grant_type,
                  scope_name: scope,
                  'response_type': response_type,
                  'username': ctx['user'],
                  'password': ctx['pass']}

        url = urljoin(conn['entry_point'], 'idsrv/connect/token')

        headers = {'Content-Type': 'application/x-www-form-urlencoded',
                   'Authorization': authorization}

        try:
            r = requests.post(url=url,
                              headers=headers,
                              data=params,
                              verify=conn['verify'])
        except Exception as e:
            raise DFViHookException(f'Ошибка получения токена:\n{e}')

        if r.status_code == requests.codes.ok:  # @UndefinedVariable
            token = r.json()
        else:
            answer = self._parse_response(r)
            msg = f'Http ошибка при полученнии токена: {r.status_code}, ответ:\n{answer}'
            raise DFViHookException(msg)

        token_type = token.get('token_type')
        token_value = token.get('access_token')
        conn['auth_string'] = f'{token_type} {token_value}'

        return conn

    def _request(self, method, url, headers=None, data=None):
        try:
            if method == 'get':
                r = requests.request(method, url, headers=headers, params=data)
            else:
                r = requests.request(method, url, headers=headers, json=data)
        except Exception as e:
            headers['Authorization'] = '*******'
            raise DFViHookException(f'{method.upper()} [{url}], хидеры:\n{headers}\nошибка:\n{e}')

        return r

    def call(self, resource='', method='get', extra_headers=None, data=None):
        """ Универсальный вызов API.
            Args:
                :resource - адрес ресурса относительно API entry point (Connection.host)
                :method - тип сообщения, обычно get или post
                :extra_headers - http заголовки, дополняющие, либо переопределяющие стандартные
                :data - полезная нагрузка запроса, как правило - словарь
            Returns:
                Ответ сервера в виде словаря
        """
        # не ддосим сервер
        current = perf_counter()
        passed = current - self.called
        if passed < self.threshold:
            sleep(self.threshold - passed)
        self.called = perf_counter()

        headers = {'X-API-VERSION': self._conn['api_version'],
                   'Authorization': self._conn['auth_string']}

        if extra_headers:
            headers.update(extra_headers)

        url = urljoin(self._conn['entry_point'], resource)

        r = self._request(method, url, headers=headers, data=data)
        answer = self._parse_response(r)

        if r.status_code == requests.codes.forbidden and 'token expired' in answer:  # @UndefinedVariable
            # пытаемся обновить токен
            self._conn = self._get_conn()
            headers = {'X-API-VERSION': self._conn['api_version'],
                       'Authorization': self._conn['auth_string']}
            r = self._request(method, url, headers=headers, data=data)
            answer = self._parse_response(r)

        if r.status_code != requests.codes.ok:  # @UndefinedVariable
            answer = self._parse_response(r)
            msg = f'{method.upper()} [{url}], код ошибки: {r.status_code}, ответ:\n<--\n{answer}\n-->'
            raise DFViHookException(msg)

        return self._parse_response(r)

    def get_error_code(self, error):
        """ Возвращает числовой http код ошибки из сообщения об ошибке. """
        code = None
        found = self._error_code_pattern.search(str(error))
        if found:
            code = int(found.groups()[-1])

        return code
