"""
Created on Apr 18, 2022

@author: pymancer@gmail.com
"""
from os import getenv
from json import loads
from box import Box
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.constants import RULES_META  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import get_binds, get_rendered_template, get_files, Timer  # @UnresolvedImport
from df.common.helpers.sql import Op  # @UnresolvedImport
from df.common.schemas.validator import SchemaValidator  # @UnresolvedImport
from df.qc import controllers  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}
binds = get_binds(local_binds=local_binds)


class QCR:
    def __init__(self, definition):
        self._validate(definition)
        self.body = Box(definition)
        self.name = self.body.name
        self.controllers = self._gen_controllers()

    @classmethod
    def get_rules(cls, rules_path=None, rules_re=None):
        """ Возвращает содержимое файла заданного скрипта. """
        af_rules_path = 'AF_RULES_PATH'

        if not rules_path:
            rules_path = getenv(af_rules_path)
        if not rules_re:
            rules_re = getenv('AF_RULES_RE')

        if not rules_path:
            msg = f'Не определена переменная окружения {af_rules_path}, отсутствует путь поиска правил ККД.'
            raise DFOperatorException(msg)

        return get_files(rules_path, reg_exp=rules_re)

    def _validate(self, definition):
        """ Валидирует json-описание ККД относительно схемы. """
        SchemaValidator().validate(definition, RULES_META)

    def _gen_controllers(self):
        generated = list()
        for item in self.body.controllers:
            controller_cls = getattr(controllers, item.family)
            generated.append(controller_cls(self.name, item))

        return generated

    def run(self):
        for controller in self.controllers:
            timer = Timer()
            controller.run()
            log.info(f'Контроллер {controller.name} выполнился за {timer()}с')


rules = QCR.get_rules()
for rule in rules:
    timer = Timer()
    rendered = get_rendered_template(rule, binds=binds)

    log.info(f'Запускается ККД из <{rule}>')
    processor = QCR(loads(rendered))
    processor.run()
    log.info(f'ККД {processor.name} выполнен за {timer()}с')

op.finish()
