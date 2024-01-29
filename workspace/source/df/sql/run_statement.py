'''
Created on 23 Jul 2019

@author: pymancer

Выполняет скрипты по выражениям, каждое в отдельной транзакции.
'''
import os
import sqlparse

from sqlalchemy import create_engine

from df.common.constants import ISOLATION_LEVELS  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import (get_conn_string, get_binds,  # @UnresolvedImport
                                       get_rendered_template, boolify,  # @UnresolvedImport
                                       get_scripts, get_df_schema)  # @UnresolvedImport
from df.common.helpers.sql import MetadataLoader, Op, execute_statements  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': op.id, 'AF_OP_INS_ID': op.ins_id}
binds = get_binds(local_binds=local_binds)

scripts = get_scripts()

is_singletran = boolify(os.getenv('AF_SINGLETRAN', default=False))
is_select = boolify(os.getenv('AF_IS_SELECT', default=False))

isolation_level = os.getenv('AF_ISOLATION_LEVEL')
if isolation_level and isolation_level.upper() in ISOLATION_LEVELS:
    isolation_level = isolation_level.upper()
else:
    isolation_level = ''

engine = create_engine(get_conn_string())

schema = get_df_schema()
job_id = os.getenv('AF_JOB_ID')
if job_id:
    metadata_loader = MetadataLoader(engine=engine, binds=binds, schema=schema)
    metadata_loader.process(os.getenv('AF_PRODUCER'), role='producer')
    metadata_loader.process(os.getenv('AF_CONSUMER'), role='consumer')


for script in scripts:
    sql = get_rendered_template(script, binds=binds)

    log.info(f'Выполняется SQL из <{script}>')

    statements = sqlparse.split(sql)

    if is_singletran:
        connection = engine.connect() if not isolation_level else engine.connect().execution_options(
            isolation_level=isolation_level)
        # Все стейтменты из одного скрипта выполняются в одной транзакции
        with connection.begin():
            results = execute_statements(statements, connection=connection, is_select=is_select)

    else:
        # Каждый стейтмент выполняется в своей транзакции
        results = execute_statements(statements, engine=engine, is_select=is_select)

    if is_select:
        log.info(f'Результат:\n{results}')


engine.dispose()
op.finish()
