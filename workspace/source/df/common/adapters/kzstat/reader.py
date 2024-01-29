import json
import requests

from json import JSONDecodeError
from urllib.parse import urlparse, parse_qs
from requests.compat import urljoin
from df.common.exceptions import DFKZStatHookException, DFAdapterException
from df.common.helpers.logger import Logger
from df.common.helpers.general import get_decoded_connection
from ..reader import AbstractReader, AbstractSheet


log = Logger()


class Hook:
    """ Взаимодействует с API Талдау """

    def __init__(self, url, token=None, api_version=None, extra=None):
        self.url = url
        self.token = token
        self.api_version = api_version
        self.extra = extra

    def _parse_response(self, response):
        content_type = response.headers.get('Content-Type')
        return response.json() if content_type and response.text and 'application/json' in content_type else response.text

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
        headers = {}
        if extra_headers:
            headers.update(extra_headers)

        url = urljoin(self.url, resource)

        try:
            r = requests.request(method, url, headers=headers, json=data, params=params)
        except Exception as e:
            raise DFKZStatHookException(f'{method.upper()} [{url}], params:\n{params}\nerror:\n{e}')

        answer = self._parse_response(r)

        if r.status_code != requests.codes.ok:
            raise DFKZStatHookException(f'{method.upper()} [{url}], код ошибки: {r.status_code}, ответ:\n<--\n{answer}\n-->')

        return answer


class KZStatSheet(AbstractSheet):
    """ "Лист" данных из API Талдау """

    def escape_quotes(self, data):
        """ Экранирует кавычки в элементах внутри json для парсинга без ошибок"""
        SEPARATORS = [',', '[', ']']
        has_near_separator = lambda d, i: d[i - 1] in SEPARATORS or d[i + 1] in SEPARATORS
        not_first_and_not_last = lambda d: index != 0 and index != len(d)
        result = ''
        for index, current in enumerate(data):
            if current == '"' and not_first_and_not_last(data) and not has_near_separator(data, index):
                current = '\\"'
            result += current
        return result

    def to_array(self):
        try:
            return list() if not len(self.data) else json.loads(self.data) if isinstance(self.data, str) else self.data
        except JSONDecodeError:
            return json.loads(self.escape_quotes(self.data))


class Reader(AbstractReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.options = kwargs.get('options')

    @property
    def sheets(self):
        if not self._sheets:
            self._sheets = {1: self.stream[0]}

        return self._sheets

    def extract_index(self, filepath):
        """ Извлекает индекс элемента из пути """
        parsed_url = urlparse(filepath)
        index_in_params = [value[0] for key, value in parse_qs(parsed_url.query).items() if
                           'index' in key or 'indicator' in key]
        index = index_in_params[0] if index_in_params else parsed_url.path.split('/')[-1]
        try:
            return int(index)
        except ValueError:
            raise DFAdapterException(f'Не удалось извлечь индекс элемента из {filepath}.')

    def build_url(self, hook, index):
        """ Формирует путь для получения записей """
        period_id = dic_id = terms = det_dic = ''
        for option in self.options:
            if option.name == 'periodId':
                period_id = option.value
            elif option.name == 'dicId':
                dic_id = option.value
            elif option.name == 'terms':
                terms = option.value
            elif option.name == 'detDic':
                det_dic = option.value
        if not period_id:
            period_id = 7
        if not dic_id:
            segments = hook.call(resource=f'ru/Api/GetSegmentList?indexId={index}&periodId={period_id}')
            if not segments:
                raise DFAdapterException(f'Для периода {period_id} в индексе {index} нет данных.')
            dic_id = max([el['dicId'] for el in segments]).replace(' + ', ',')

        params = [f"indicators={index}", f"dics={dic_id}", f"periodId={period_id}"]
        if terms:
            params.append(f"terms={terms}")
        if det_dic:
            params.append(f"detDic={det_dic}")
        return f'ru/PivotGrid/getPivotData?{"&".join(params)}'

    def get_stream(self, path, **kwargs):
        """ Возвращает "поток" из источника в виде списка с одним "листом" данных. """
        conn = get_decoded_connection(postfix='PRODUCER')
        hook = Hook(url=conn['host'])
        index = self.extract_index(path)
        url = self.build_url(hook, index)

        log.info(f'Загрузка данных с {url}')
        sheet = KZStatSheet(hook.call(resource=url))

        self.stream = [sheet]

        return self.stream

    def get_rows(self, **kwargs):
        sheet = kwargs.get('sheet', 0)
        self.sheet_stream = self.stream

        return self.sheet_stream[sheet].to_array()
