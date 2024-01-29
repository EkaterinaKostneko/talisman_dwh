from df.common.schemas.model import LocalEntity, ViTableLocalEntity  # @UnresolvedImport
from df.common.exceptions import DFUnsupportedEntityException  # @UnresolvedImport
from .sql.uploader import SQLUploader
from .viqube.uploader import ViqubeUploader


def get_uploader(entity, **kwargs):
    entity_type = type(entity)

    if entity_type is LocalEntity:
        uploader = SQLUploader(entity, **kwargs)
    elif entity_type is ViTableLocalEntity:
        uploader = ViqubeUploader(entity, **kwargs)
    else:
        raise DFUnsupportedEntityException(f'Не найден аплоадер для типа сущности: {entity_type}')

    return uploader
