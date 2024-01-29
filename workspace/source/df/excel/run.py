'''
Created on 02 Jun 2019

@author: pymancer
'''
import sys

from os import getenv
from extractor import Extractor  # @UnresolvedImport
from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import get_binds, get_cdm  # @UnresolvedImport
from df.common.helpers.metadata import process_entity  # @UnresolvedImport
from df.common.helpers.sql import Op  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}
binds = get_binds(local_binds=local_binds)

producer = get_cdm(getenv('AF_PRODUCER'), binds=binds)
consumer = get_cdm(getenv('AF_CONSUMER'), binds=binds)

# должны обрабатываться только совпадающие по наименованию сущности
matched_entities = []
for producer_entity in producer.entities:
    for consumer_entity in consumer.entities:
        consumer_entity = process_entity(consumer_entity, consumer, binds=binds)
        if producer_entity.name == consumer_entity.name:
            matched_entities.append((producer_entity, consumer_entity))
            break

if matched_entities:
    for producer_entity, consumer_entity in matched_entities:
        extractor = Extractor(getenv('AF_TASK_ID'), getenv('AF_RUN_ID'), producer_entity, consumer_entity,
                              op_id=local_binds['AF_OP_ID'], op_ins_id=local_binds['AF_OP_INS_ID'])
        extractor.run()
else:
    log.info('В получателе и источнике нет пересекающихся сущностей. Загружать нечего.')

op.finish()

sys.exit(0)
