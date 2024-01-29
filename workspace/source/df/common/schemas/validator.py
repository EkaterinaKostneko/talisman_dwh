"""
Created on Aug 14, 2019

@author: pymancer

Модуль управление валидацией CDM JSON файлов.
"""
import os.path

from json import load
from jsonschema import Draft202012Validator
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.constants import MODELS_META, RULES_META  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport

log = Logger()


class SchemaValidator:
    """ Глобальный набор валидаторов CDM схем. """
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SchemaValidator, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self._validators = dict()
        current_dir_path = os.path.dirname(os.path.abspath(__file__))
        schemas_path = os.path.join(current_dir_path, 'static')

        schema_path = os.path.join(schemas_path, f'{MODELS_META}.json')
        with open(schema_path) as f:
            raw_schema = load(f)
        self._validators[MODELS_META] = Draft202012Validator(raw_schema)

        schema_path = os.path.join(schemas_path, f'{RULES_META}.json')
        with open(schema_path) as f:
            raw_schema = load(f)
        self._validators[RULES_META] = Draft202012Validator(raw_schema)

    def validate(self, json_data, family):
        """ Валидирует JSON модель в соответствии с заданной схемой.
        """
        invalid = False

        for error in sorted(self._validators[family].iter_errors(json_data), key=str):
            invalid = True
            log.error(error.message)

        if invalid:
            raise DFOperatorException('Ошибка валидации метаданных')

        return json_data
