'''
Created on 06 Feb 2020

@author: vmikhaylov
'''
import os

from df.common.helpers.logger import Logger  # @UnresolvedImport
from df.common.helpers.general import get_cdm  # @UnresolvedImport
from df.common.schemas.model import ReferenceEntity  # @UnresolvedImport

log = Logger()


def process_entity(entity, cdm_model, binds=None):
    if isinstance(entity, ReferenceEntity):
        for reference_model in cdm_model.referenceModels:
            if entity.modelId == reference_model.id:
                return get_reference_entity(reference_model.location, entity_name=entity.name,
                                            entity_source=entity.source, binds=binds)

    return entity


def get_reference_entity(cdm_path, entity_name=None, entity_source=None, binds=None):
    cdm_model = load_model(cdm_path, binds=binds)

    log.info(f'Поиск сущности <{entity_source}> (entity_name: <{entity_name}>) в метаданных модели <{cdm_model.name}>.')
    for entity in cdm_model.entities:
        if entity_source and entity_source == entity.name:
                # ссылочная модель, обрабатываем под наименованием, используемым ссылающейся моделью
                entity.name = entity_name
                return entity
    return None


def load_model(cdm_path, binds=None):
    cdm_path = os.path.abspath(cdm_path)
    log.info(f'Загружается модель из <{cdm_path}>.')
    return get_cdm(cdm_path, binds=binds)
