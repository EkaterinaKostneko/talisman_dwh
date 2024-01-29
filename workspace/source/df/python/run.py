"""
Created on 21 Jan 2020

@author: pymancer
"""
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import get_binds, get_rendered_template, get_scripts  # @UnresolvedImport
from df.common.helpers.sql import Op  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}
binds = get_binds(local_binds=local_binds)

scripts = get_scripts()

for script in scripts:
    rendered = get_rendered_template(script, binds=binds)

    log.info(f'Запускается python из <{script}>')
    log.debug(f'Скрипт:\n{rendered}')

    exec(rendered)

op.finish()
