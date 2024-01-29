"""
Created on Aug 8, 2019

@author: pymancer

Пересоздает процедуры по шаблону, выполняет их, затем - удаляет.
Каждая процедура должна быть в своем файле и без схемы называться в точности как имя файла без расширения.

Если в переменных окружения отсутствует AF_JOB_ID, то просто выполняет SQL из файла.
"""
import os

from sqlalchemy import create_engine
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.general import (get_conn_string, get_binds, get_files,  # @UnresolvedImport
                                       get_rendered_template, get_df_schema)  # @UnresolvedImport
from df.common.helpers.sql import execute_sql, MetadataLoader, Op  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': op.id, 'AF_OP_INS_ID': op.ins_id}
binds = get_binds(local_binds=local_binds)

af_procedure_path = 'AF_PROCEDURE_PATH'

procedure_path = os.getenv(af_procedure_path)
procedure_re = os.getenv('AF_PROCEDURE_RE')

context_table = 'df_entity_ctx'
context_relation_table = 'df_entity_relation_ctx'

if not procedure_path:
    msg = f'Не определена переменная окружения {af_procedure_path}, отсутствует путь для поиска процедур.'
    raise DFOperatorException(msg)

files = get_files(procedure_path, reg_exp=procedure_re)

af_job_id = 'AF_JOB_ID'
job_id = os.getenv(af_job_id)
schema = get_df_schema()

if not job_id:
    log.warning(f'Отсутствует переменная окружения {af_job_id}, будет создана статичная процедура.')

engine = create_engine(get_conn_string())

if job_id:
    metadata_loader = MetadataLoader(engine=engine, binds=binds, schema=schema)
    metadata_loader.process(os.getenv('AF_PRODUCER'), role='producer')
    metadata_loader.process(os.getenv('AF_CONSUMER'), role='consumer')

for file in files:
    procedure = f"{binds['AF_SC_']}{os.path.splitext(os.path.basename(file))[0]}_{job_id}"

    log.info(f'Создается процедура <{procedure}>')
    sql = get_rendered_template(file, binds=binds)
    execute_sql(sql, engine=engine)

    if job_id:
        log.info(f'Выполняется процедура <{procedure}>')
        # TODO: переделать на вызов процедуры из SQLAlchemy
        sql = f'exec {procedure};'
        execute_sql(sql, engine=engine)

        log.info(f'Удаляется процедура <{procedure}>')
        sql = f'drop procedure {procedure};'
        execute_sql(sql, engine=engine)

engine.dispose()
op.finish()
