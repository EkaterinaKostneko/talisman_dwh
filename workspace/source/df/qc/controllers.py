"""
Created on Apr 20, 2022

@author: pymancer@gmail.com
"""
from abc import ABC, abstractmethod
from collections import defaultdict
from box import Box
from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker, mapper
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import Timer, get_df_schema, get_db_schema  # @UnresolvedImport
from df.common.helpers.sql import (execute_sql, get_engine_metadata,  # @UnresolvedImport
                                   create_engine, get_conn_string,  # @UnresolvedImport
                                   Op, session_scope)  # @UnresolvedImport
from df.qc import checkers  # @UnresolvedImport

log = Logger()

ROW_CHECKERS = (
    'must_be_not_null',
    'must_be_shorter_than',
    'must_be_longer_than',
    'must_be_less_than',
    'must_be_more_than',
)
COLUMN_CHECKERS = (
    'max_repeats',
)
RELATION_CHECKERS = (
    'must_be_known',
)

SEVERITIES = {
    'critical': 0,
    'high': 1,
    'medium': 2,
    'low': 3
}

QUERY = """select ctid, {attributes_str} from (
select ctid,
       {subquery}
  from {entity}
) t
 where coalesce({attributes_str}) is not null
"""

ROW_CASE_QUERY = """case {whens}
            else null
        end {attribute}"""

WHEN_QUERY = "when {checker} then '{unit}'"

def sort_units(unit):
    return unit.priority + SEVERITIES[unit.severity]

def filter_row_units(unit):
    return True if unit.enabled and unit.checker.name in ROW_CHECKERS else False

def filter_column_units(unit):
    return True if unit.enabled and unit.checker.name in COLUMN_CHECKERS else False

def filter_relation_units(unit):
    return True if unit.enabled and unit.checker.name in RELATION_CHECKERS else False


class AbstractController(ABC):
    def __init__(self, qc, definition, **kwargs):
        self.qc = qc
        self.body = definition

    @abstractmethod
    def run(self):
        """ Выполняет процесс контроля. """


class ServersideCaseQuerier(AbstractController):
    def __init__(self, qc, definition, **kwargs):
        ins_id = Op().ins_id
        if not (ins_id := Op().ins_id):
            raise DFOperatorException('Не определен идентификатор шага процесса. Контроль качества невозможен.')

        super().__init__(qc, definition, **kwargs)

        self.engine = create_engine(get_conn_string())
        df_schema = get_df_schema()
        df_schema_ = get_db_schema(schema=df_schema, dotted=True)

        errors_table = self.body.context.get('errors_entity')  or f'{df_schema_}qc_error'
        metadata = get_engine_metadata(engine=self.engine, schema=df_schema)

        if '.' in errors_table:
            errors_table_parts = errors_table.split('.')
            errors_table = errors_table_parts[-1]
            errors_schema = errors_table_parts[-2]
        else:
            errors_schema = None

        if errors_schema == df_schema:
            errors_metadata = metadata
        else:
            errors_metadata = get_engine_metadata(engine=self.engine, schema=errors_schema)

        self.name = self.body.name
        self.ctx = Box({'op_ins_id': ins_id,
                        'df_schema_': df_schema_,
                        'entity': self.body.context.entity})

        self.session_class = sessionmaker(bind=self.engine)

        self.t_qc = Table('qc', metadata, autoload=True)
        self.c_qc = type('Qc', (), {})
        mapper(self.c_qc, self.t_qc)

        self.t_error = Table(errors_table, errors_metadata, autoload=True)
        self.c_error = type(errors_table.capitalize(), (), {})
        mapper(self.c_error, self.t_error)

    def _get_case_subquery(self, attributes):
        result = list()
        for attribute in attributes:
            whens = list()
            for item in attributes[attribute]:
                checker = getattr(checkers, item.checker.name)
                when = WHEN_QUERY.format(checker=checker(item.checker, self.ctx), unit=item.name)
                whens.append(when)

            result.append(ROW_CASE_QUERY.format(whens='\n            '.join(whens), attribute=attribute))

        return result

    def _gen_subquery(self, attributes, qc_type):
        """ Генерирует sql запрос по json-описанию правил. """
        result = list()

        if attributes:
            timer = Timer()
            result = self._get_case_subquery(attributes)

            log.debug(f'Генерация запроса правил {self.name} типа {qc_type} выполнена за {timer()}с')
        else:
            log.debug(f'Правила типа {qc_type} в {self.name} отсутствуют')

        return result

    def _get_data(self, query):
        """ Выполняет запрос контроля и возвращает результаты. """
        data = list()

        if query:
            timer = Timer()
            data = execute_sql(query, get_select=True, with_headers=True)

            log.debug(f'Запрос qc {self.name} выполнен за {timer()}с')
            if not data:
                log.debug(f'Результаты контроля {self.name} отсутствуют')

        return data

    def _save_data(self, data):
        """ Сохраняет результаты контроля в системные таблицы. """
        if data:
            timer = Timer()

            with session_scope(self.session_class) as s:
                qc_record_id = None
                units = dict()

                for record in data[1:]:
                    location = record[0][1:-1].split(',')
                    partition = int(location[0])
                    row = int(location[1])

                    for idx, cell in enumerate(record):
                        if idx == 0 or not cell:  # ctid or no error
                            continue

                        unit = (self.qc, self.name, cell)

                        if not (qc_record_id := units.get(unit)):
                            qc_record = self.c_qc()
                            qc_record.op_ins_id = self.ctx.op_ins_id
                            qc_record.name = self.qc
                            qc_record.controller = self.name
                            qc_record.unit = cell
                            s.add(qc_record)
                            s.flush()
                            qc_record_id = qc_record.id
                            units[unit] = qc_record_id

                        column = data[0][idx]
                        err_record = self.c_error()
                        err_record.qc_id = qc_record_id
                        err_record.col = column
                        err_record.row = row
                        err_record.partition = partition
                        s.add(err_record)

            log.debug(f'Результаты контроля {self.name} сохранены за {timer()}с')

    def _get_attributes(self, units):
        attributes = defaultdict(lambda: list())
        for unit in units:
            attributes[unit.checker.parameters.attribute].append(unit)

        return attributes

    def _get_query(self, attributes, subqueries):
        attributes_str = ', '.join(attributes)
        query = QUERY.format(attributes_str=attributes_str,
                             subquery=',\n       '.join(subqueries),
                             entity=self.ctx.entity)

        return query

    def _subrun(self, units, qc_type):
        attributes = self._get_attributes(units)
        subqueries = self._gen_subquery(attributes, qc_type)
        data = self._get_data(self._get_query(attributes, subqueries))
        self._save_data(data)

    def run(self):
        rows = sorted(filter(filter_row_units, self.body.units), key=sort_units)
        self._subrun(rows, 'rows')

        columns = sorted(filter(filter_column_units, self.body.units), key=sort_units)
        self._subrun(columns, 'columns')

        relations = sorted(filter(filter_relation_units, self.body.units), key=sort_units)
        self._subrun(relations, 'relations')
