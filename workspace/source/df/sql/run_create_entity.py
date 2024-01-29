'''
Created on 29 Jul 2019

@author: pymancer

Создание таблицы/таблиц из CDM модели.
Порядок создания объектов определяется алфавитным порядком файлов метаданных.
'''
import os

from sqlalchemy import create_engine

from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import get_conn_string, get_binds, get_files, get_as_int  # @UnresolvedImport
from df.common.helpers.sql import MetadataWarlock, Op  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}
binds = get_binds(local_binds=local_binds)

af_metadata_path = 'AF_METADATA_PATH'
metadata_path = os.getenv(af_metadata_path)
metadata_re = os.getenv('AF_METADATA_RE')
metadata_depth = get_as_int(os.getenv('AF_METADATA_DEPTH'))

if not metadata_path:
    msg = f'Не определена переменная окружения {af_metadata_path}, отсутствует путь для поиска метаданных.'
    raise DFOperatorException(msg)

metadata_files = get_files(metadata_path, reg_exp=metadata_re, recursive=True, level=metadata_depth)

engine = create_engine(get_conn_string())

warlock = MetadataWarlock(engine=engine, binds=binds)

for metadata_file in sorted(metadata_files):
    log.info(f'Создание сущностей. Обрабатывается файл {metadata_file}.')
    warlock.process(metadata_file)

engine.dispose()
op.finish()
