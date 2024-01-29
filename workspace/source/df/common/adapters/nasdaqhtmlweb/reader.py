import requests
import pyexcel

from requests.compat import urljoin
from df.common.helpers.logger import Logger
from df.common.helpers.general import get_decoded_connection
from df.common.exceptions import DFNasdaqHtmlWebHookException, DFAdapterException
from ..reader import AbstractReader

log = Logger()


class Hook:
    """ Взаимодействует с http сервиса nasdaq"""

    def __init__(self, url):
        self.url = url

    @staticmethod
    def _parse_response(response):
        content_type = response.headers.get('Content-Type')
        if content_type and response.text:
            return response.text

    def call(self, resource='', method='post', headers=None, data=None):
        """
        Вызов post запроса по адресу с сервиса nasdaq
        method: тип запроса, как правило get или post
        :extra_headers - http заголовки, дополняющие, либо переопределяющие стандартные
        :data - полезная нагрузка запроса, как правило - словарь
        """

        url = urljoin(self.url, resource)
        try:
            r = requests.request(method, url, headers=headers, data=data)
        except Exception as e:
            raise DFNasdaqHtmlWebHookException(f'{method.upper()} [{self.url}], data:\n{data}\nerror:\n{e}')

        answer = self._parse_response(r)

        if r.status_code != requests.codes.ok:
            msg = f'{method.upper()} [{self.url}], код ошибки: {r.status_code}, ответ:\n<--\n{answer}\n-->'
            raise DFNasdaqHtmlWebHookException(msg)

        return answer


class Reader(AbstractReader):
    """ Ридер ответов от сервиса nasdaq"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.options = kwargs.get('options')

    @property
    def sheets(self):
        return self.stream.sheets

    def get_stream(self, path, **kwargs):
        """ Возвращает "поток" из источника в виде списка с одним "листом" данных. """
        params = {
            'Exchange': 'NMF',
            'SubSystem': 'History',
            'Action': 'GetMarket',
            'Market': 'GITS:NC:ENO',
            'inst__an': 'id,nm,upc,tp,ed,fnm',
            'inst__e': '9',
            'hi__a': '31,5,6,29,1,2,8,60,19,57,58,9,30,20',
            'empdata': 'false',
            'ext_xslt': 'nordpool-v2/inst_hi_tables_1_new.xsl',
            'ext_xslt_options': ',summary,',
            'ext_xslt_notlabel': 'fnm',
            'ext_xslt_lang': 'en',
            'ext_xslt_tableId': 'historyTable',
            'ext_xslt_tableClass': 'tablesorter',
            'ext_xslt_market': 'GITS:NC:ENO',
            'app': 'www.nasdaqomx.com//commodities/market-prices/history',
        }
        conn = get_decoded_connection(postfix='PRODUCER')
        hook = Hook(url=conn['host'])

        if self.options:
            for option in self.options:
                value = option.value

                if option.name in params:
                    if isinstance(params[option.name], list):
                        params[option.name].append(value)
                    else:
                        params[option.name] = [params[option.name], value]
                else:
                    params[option.name] = value

        data = 'xmlquery=<post>'
        for item in params.items():
            data += f'<param name="{item[0]}" value="{item[1]}"/>'
        data += '</post>'

        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

        content = hook.call(resource=path, headers=headers, data=data)

        self.stream = pyexcel.iget_book(file_type="html", file_content=content)

        return self.stream

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

        self.sheet_stream = self.stream[sheet_index]
        return pyexcel.iget_array(sheet_stream=self.sheet_stream, library='pyexcel-htmlr')