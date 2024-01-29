'''
Created on 23 Jul 2019

@author: pymancer
'''
import os

from sqlalchemy import create_engine

from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import (get_conn_string, get_binds,  # @UnresolvedImport
                                       get_rendered_template, boolify,  # @UnresolvedImport
                                       get_scripts, get_df_schema)  # @UnresolvedImport
from df.common.helpers.sql import (process_sql, execute_sql,  # @UnresolvedImport
                                   MetadataLoader, Op)  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': op.id, 'AF_OP_INS_ID': op.ins_id}
binds = get_binds(local_binds=local_binds)

engine = create_engine(get_conn_string())

schema = get_df_schema()
job_id = os.getenv('AF_JOB_ID')
if job_id:
    metadata_loader = MetadataLoader(engine=engine, schema=schema, binds=binds)
    metadata_loader.process(os.getenv('AF_PRODUCER'), role='producer')
    metadata_loader.process(os.getenv('AF_CONSUMER'), role='consumer')

nocount = boolify(os.getenv('AF_NOCOUNT')) if os.getenv('AF_NOCOUNT') is not None else True

scripts = get_scripts()

for script in scripts:
    sql = get_rendered_template(script, binds=binds)

    log.info(f'Выполняется SQL из <{script}>')

    sql = process_sql(sql, engine=engine, nocount=nocount)
    try:
        execute_sql(sql, engine=engine)
    except Exception as e:
        engine.dispose()
        raise

engine.dispose()
op.finish()
