from .netdb.reader import Reader as NetDB
from .pyexcel.reader import Reader as PyExcel
from .pipware.reader import Reader as PIPware
from .viapi.reader import Reader as ViAPI
from .eb.reader import Reader as EB
from .ebweb.reader import Reader as EBWeb
from .msheu.reader import Reader as MSHEU
from .kzstat.reader import Reader as KZStat
from .excel2003xml.reader import Reader as Excel2003XML
from .jsonmap.reader import Reader as JSONMap
from .jsonlist.reader import Reader as JSONList
from .kztask.reader import Reader as KZTask
from .nasdaqhtmlweb.reader import Reader as NasdaqHtmlWeb
from .content.reader import Reader as Content
from df.common.schemas.model import (NetDBFileEntity, ExcelFileEntity, Excel2003XMLFileEntity,  # @UnresolvedImport
                                     CSVFileEntity, PIPwareWebEntity, DCDimensionEntity, JSONMapWebEntity,  # @UnresolvedImport
                                     EBFileEntity, EBWebEntity, MSHEUFileEntity, KZStatWebEntity, JSONListWebEntity,  # @UnresolvedImport
                                     KZTaskWebEntity, NasdaqHTMLWebEntity, HTMLFileEntity, HTMLWebEntity,  # @UnresolvedImport
                                     ContentFileEntity)  # @UnresolvedImport

from df.common.exceptions import DFUnsupportedEntityException  # @UnresolvedImport


def get_entity_reader(entity, **kwargs):
    entity_type = type(entity)

    if entity_type is NetDBFileEntity:
        library = NetDB()
    elif entity_type is ExcelFileEntity:
        if kwargs.get('load_extra', False):
            library = PyExcel(flavor='openpyxl')
        else:
            if kwargs.get('low_memory_mode', False):
                library = PyExcel(flavor='pyexcel-xlsxr')
            else:
                library = PyExcel(flavor='pyexcel-xlsx')
    elif entity_type is CSVFileEntity:
        library = PyExcel(flavor='pyexcel-csvr')
    elif entity_type is HTMLFileEntity or entity_type is HTMLWebEntity:
        library = PyExcel(flavor='pyexcel-htmlr')
    elif entity_type is ContentFileEntity:
        library = Content(**kwargs)
    elif entity_type is PIPwareWebEntity:
        library = PIPware(options=entity.options)
    elif entity_type is DCDimensionEntity:
        library = ViAPI()
    elif entity_type is EBFileEntity:
        library = EB()
    elif entity_type is EBWebEntity:
        library = EBWeb(options=entity.options)
    elif entity_type is MSHEUFileEntity:
        library = MSHEU()
    elif entity_type is KZStatWebEntity:
        library = KZStat(options=entity.options)
    elif entity_type is Excel2003XMLFileEntity:
        library = Excel2003XML()
    elif entity_type is JSONMapWebEntity:
        library = JSONMap()
    elif entity_type is JSONListWebEntity:
        library = JSONList()
    elif entity_type is KZTaskWebEntity:
        library = KZTask(options=entity.options)
    elif entity_type is NasdaqHTMLWebEntity:
        library = NasdaqHtmlWeb(options=entity.options)
    else:
        raise DFUnsupportedEntityException(f'Не найден адаптер для типа сущности: {entity_type}')

    return library
