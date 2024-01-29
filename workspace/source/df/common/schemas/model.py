"""
Created: 2019-06-09

@author: pymancer

DF CDM Library

Based on: https://github.com/microsoft/CDM/blob/master/samples/cdm-folders/python/CdmModel.py
"""
import re
import json

from enum import Enum
from collections import OrderedDict
from df.common.constants import MODELS_META  # @UnresolvedImport
from df.common.schemas.validator import SchemaValidator  # @UnresolvedImport


def getattrIgnoreCase(obj, attr, default=None):
    for i in dir(obj):
        if i.lower() == attr.lower():
            return getattr(obj, attr, default)
    return default


class SchemaEntry(object):
    __unassigned = object()

    def __init__(self, name, cls, defaultValue=None, verbose=False):
        self.name = name
        self.cls = cls
        if defaultValue is None and issubclass(cls, list):
            defaultValue = cls()
        self.defaultValue = defaultValue
        self.verbose = verbose

    def shouldSerialize(self, value):
        if self.verbose:
            return True
        if issubclass(self.cls, list):
            return len(value) > 0
        return self.defaultValue != value


class PolymorphicMeta(type):
    classes = {}

    def __new__(cls, name, bases, attrs):
        cls = type.__new__(cls, name, bases, attrs)
        cls.classes[cls] = {cls.__name__: cls}  # TODO: abstract?
        cls.__appendBases(bases, cls)
        return cls

    @staticmethod
    def __appendBases(bases, cls):
        for base in bases:
            basemap = cls.classes.get(base, None)
            if basemap is not None:
                basemap[cls.__name__] = cls
                cls.__appendBases(base.__bases__, cls)


class Polymorphic(metaclass=PolymorphicMeta):
    @classmethod
    def fromJson(cls, value):
        actualClass = PolymorphicMeta.classes[cls][value["$type"]]
        return super(Polymorphic, actualClass).fromJson(value)


class Base(object):
    __ctors = {}
    schema = ()

    def __init__(self):
        for entry in self.schema:
            setattr(self, entry.name, entry.defaultValue)

    @classmethod
    def fromJson(cls, value):
        result = cls()
        for entry in cls.schema:
            element = value.pop(entry.name, result)
            if element != result:
                setattr(result, entry.name, cls.__getCtor(entry.cls)(element))
        result.customProperties = value
        return result

    @classmethod
    def __getCtor(cls, type_):
        ctor = cls.__ctors.get(type_, None)
        if not ctor:
            ctor = getattr(type_, "fromJson", type_)
            cls.__ctors[type_] = ctor
        return ctor

    def validate(self):
        tmp = object()
        className = self.__class__.__name__
        for entry in self.schema:
            element = getattrIgnoreCase(self, entry.name, tmp)
            if element != tmp and element is not None:
                if not isinstance(element, entry.cls):
                    raise TypeError("%s.%s must be of type %s" %
                                    (className, entry.name, entry.cls))
                getattr(element, "validate", lambda: None)()

    def toJson(self):
        result = OrderedDict()
        if isinstance(self, Polymorphic):
            result["$type"] = self.__class__.__name__
        for entry in self.schema:
            element = getattrIgnoreCase(self, entry.name, result)
            if element != result and entry.shouldSerialize(element):
                result[entry.name] = getattr(
                    element, "toJson", lambda: element)()
        result.update(getattrIgnoreCase(self, "customProperties", {}))
        return result


class ObjectCollection(list, Base):
    def append(self, item):
        if not isinstance(item, self.itemType):
            raise TypeError("item is not of type %s" % self.itemType)
        super(ObjectCollection, self).append(item)

    @classmethod
    def fromJson(cls, value):
        result = cls()
        ctor = getattr(cls.itemType, "fromJson", cls.itemType)
        for item in value:
            super(ObjectCollection, result).append(ctor(item))
        return result

    def toJson(self):
        result = []
        for item in self:
            result.append(getattr(item, "toJson", lambda: item)())
        return result

    def validate(self):
        for item in self:
            # есть списки, которые содержат просто строки, например, schemas
            if not isinstance(item, str):
                item.validate()


String = str
Uri = str
DateTimeOffset = str


class JsonEnum(Enum):
    def toJson(self):
        return self.value


class CsvQuoteStyle(JsonEnum):
    Csv = "QuoteStyle.Csv"
    None_ = "QuoteStyle.None"


class CsvStyle(JsonEnum):
    QuoteAlways = "CsvStyle.QuoteAlways"
    QuoteAfterDelimiter = "CsvStyle.QuoteAfterDelimiter"


class DataType(JsonEnum):
    # TODO: Fix autogeneration
    Unclassified = "unclassified"
    String = "string"
    Int64 = "int64"
    Double = "double"
    DateTime = "dateTime"
    DateTimeOffset = "dateTimeOffset"
    Decimal = "decimal"
    Boolean = "boolean"
    Guid = "guid"
    Json = "json"
    Text = "text"
    Date = "date"
    Timestamp = "timestamp"
    Varchar = "varchar"
    Unicode = "unicode"
    Integer = "integer"
    Float = "float"
    # viqube specific
    Real = "real"
    Short = "short"
    Long = "long"
    Time = "time"


class Annotation(Base):
    schema = Base.schema + (
        SchemaEntry("name", String),
        SchemaEntry("value", String)
    )

    def validate(self):
        super().validate()
        className = self.__class__.__name__
        if not self.name:
            raise ValueError("%s.name is not set." % (className, ))


class Option(Annotation):
    """ Option object compatible with Annotation structure. """


class AnnotationCollection(ObjectCollection):
    itemType = Annotation


class OptionCollection(ObjectCollection):
    itemType = Option


class MetadataObject(Base):
    schema = Base.schema + (
        SchemaEntry("name", String),
        SchemaEntry("description", String),
        SchemaEntry("annotations", AnnotationCollection)
    )

    nameLengthMin = 1
    nameLengthMax = 256
    invalidNameRegex = re.compile("^\\s|\\s$")
    descriptionLengthMax = 4000

    def __repr__(self):
        name = getattr(self, "name", None)
        className = self.__class__.__name__
        if name:
            return "<%s '%s'>" % (className, name)
        else:
            return "<%s>" % (className, )

    def validate(self):
        super().validate()
        className = self.__class__.__name__
        if self.name is not None:
            if len(self.name) > self.nameLengthMax or len(self.name) < self.nameLengthMin:
                raise ValueError("Length of %s.name (%d) is not between %d and %d." % (className, len(self.name), self.nameLengthMin, self.nameLengthMax))  # noqa
            if self.invalidNameRegex.search(self.name):
                raise ValueError("%s.name cannot contain leading or trailing blank spaces or consist only of whitespace." % (className, ))  # noqa
        if self.description is not None and len(self.description) > self.descriptionLengthMax:
            raise ValueError("Length of %s.description (%d) may not exceed %d." % (className, len(self.name), self.nameLengthMin, self.nameLengthMax))  # noqa

    def pytonize_value(self, value):
        """ Приводит специальные значения аннотаций к соответствующим типам Python.
        """
        if value in ('true', 'True'):
            value = True
        elif value in ('false', 'False'):
            value = False
        elif value in ('null', 'None'):
            value = None

        return value

    def get_note(self, name, default=None):
        """ Возвращает значение аннотации по ее имени.
            Может вернуть заданное значение по умолчанию.
        """
        value = default
        name = name.lower()
        for annotation in self.annotations:
            if annotation.name.lower() == name:
                value = self.pytonize_value(annotation.value)
                break
        return value

    def find_by_note(self, collection, name, value):
        """ Ищет элемент указанной коллекции по имени и значению его аннотации. """
        if not hasattr(self, collection):
            raise KeyError(
                f'{self.__class__.__name__} does not have {collection}')

        for item in getattr(self, collection):
            for annotation in item.annotations:
                if annotation.name == name and annotation.value == value:
                    return item
        return None

    def get_notes(self, defaults=None):
        """ Возвращает словарь аннотаций элемента.
            Args:
                :defaults - dict, если передан, использует значения в нем как дефолтные
            Returns:
                Словарь аннотаций.
            TODO: добавить валидатор моделей и использовать его, как источник дефолтных значений
        """
        annotations = dict()
        for annotation in self.annotations:
            annotations[annotation.name] = self.pytonize_value(
                annotation.value)

        if defaults:
            result = {k.lower(): v for k, v in defaults.items()}
        else:
            result = dict()

        result.update(annotations)
        return result

    def merge_notes(self, annotations):
        """ Объединяет словарь аннотаций элемента с переданным
        """
        add_annotations = list()
        for in_note in annotations:
            add_flag = True
            for self_note in self.annotations:
                if in_note.name == self_note.name:
                    self_note.value = in_note.value
                    add_flag = False
            if add_flag:
                add_annotations.append(in_note)
        for in_note in add_annotations:
            self.annotations.append(in_note)


class MetadataObjectCollection(ObjectCollection):
    def __getitem__(self, index):
        if type(index) == str:
            index = next((i for i, item in enumerate(self)
                          if item.name.lower() == index.lower()), None)
        if index is None:
            return None
        return super(MetadataObjectCollection, self).__getitem__(index)

    def validate(self):
        super().validate()
        className = self.__class__.__name__
        s = set()
        for item in self:
            if item.name is not None and item.name in s:
                raise ValueError(
                    "%s contains non-unique item name '%s'" % (className, item.name))
            s.add(item.name)


class DataObject(MetadataObject):
    schema = MetadataObject.schema + (
        SchemaEntry("isHidden", bool, False),
    )

    def validate(self):
        super().validate()
        className = self.__class__.__name__
        if self.name is None:
            raise ValueError("%s.name is not set" % (className, ))


class SchemaCollection(ObjectCollection):
    itemType = Uri


class Reference(Base):
    schema = Base.schema + (
        SchemaEntry("id", String),
        SchemaEntry("location", Uri)
    )


class ReferenceCollection(ObjectCollection):
    itemType = Reference


class AttributeReference(Base):
    schema = Base.schema + (
        SchemaEntry("entityName", String),
        SchemaEntry("attributeName", String)
    )

    def __str__(self):
        return f'{self.entityName}.{self.attributeName}'

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def validate(self):
        super().validate()
        className = self.__class__.__name__
        if not self.entityName:
            raise ValueError("%s.entityName is not set" % (className, ))
        if not self.attributeName:
            raise ValueError("%s.attributeName is not set" % (className, ))


class Relationship(Polymorphic, MetadataObject):
    schema = MetadataObject.schema

    def __repr__(self):
        return "<[%s]>" % (getattr(self, "name", "(unnamed)"), )


class SingleKeyRelationship(Relationship):
    schema = Relationship.schema + (
        SchemaEntry("fromAttribute", AttributeReference),
        SchemaEntry("toAttribute", AttributeReference)
    )

    def __str__(self):
        return f'{str(self.fromAttribute)} -> {str(self.toAttribute)}'

    def __hash__(self):
        return hash(f'{hash(self.fromAttribute)}.{hash(self.toAttribute)}')

    def __eq__(self, other):
        return isinstance(other, self.__class__) and hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def validate(self):
        super().validate()
        className = self.__class__.__name__
        if self.fromAttribute is None:
            raise ValueError("%s.fromAttribute is not set" % (className, ))
        if self.toAttribute is None:
            raise ValueError("%s.toAttribute is not set" % (className, ))
        if self.fromAttribute == self.toAttribute:
            raise ValueError(
                "%s must exist between different attribute references" % (className, ))


class RelationshipCollection(ObjectCollection):
    itemType = Relationship


class FileFormatSettings(Polymorphic, Base):
    pass


class CsvFormatSettings(FileFormatSettings):
    schema = FileFormatSettings.schema + (
        SchemaEntry("columnHeaders", bool, False),
        SchemaEntry("delimiter", String, ","),
        SchemaEntry("quoteStyle", CsvQuoteStyle, CsvQuoteStyle.Csv),
        SchemaEntry("csvStyle", CsvStyle, CsvStyle.QuoteAlways),
    )


class Partition(DataObject):
    schema = DataObject.schema + (
        SchemaEntry("refreshTime", DateTimeOffset),
        SchemaEntry("location", Uri),
        SchemaEntry("fileFormatSettings", FileFormatSettings)
    )


class PartitionCollection(MetadataObjectCollection):
    itemType = Partition


class Attribute(MetadataObject):
    schema = MetadataObject.schema + (
        SchemaEntry("dataType", DataType, defaultValue=DataType.Unclassified),
    )

    def __repr__(self):
        return "<[%s]>" % (getattr(self, "name", "(unnamed)"), )


class FileAttribute(Attribute):
    schema = Attribute.schema + (
        SchemaEntry("ordinal", int),
    )


class ExcelFileAttribute(FileAttribute):
    schema = FileAttribute.schema + (
        SchemaEntry("columnLetter", String),
        SchemaEntry("columnName", String)
    )


class HTMLFileAttribute(FileAttribute):
    schema = FileAttribute.schema + (
        SchemaEntry("columnName", String),
    )


class ContentFileAttribute(FileAttribute):
    schema = FileAttribute.schema + (
        SchemaEntry("columnName", String),
    )


class NetDBFileAttribute(FileAttribute):
    schema = FileAttribute.schema + (
        SchemaEntry("columnName", String),
    )


class CSVFileAttribute(FileAttribute):
    # Возможно стоит использовать CsvFormatSettings
    # но они для Partition, которые на данный момент не используются
    schema = NetDBFileAttribute.schema


class PIPwareAttribute(NetDBFileAttribute):
    pass


class DCDimensionAttribute(NetDBFileAttribute):
    pass


class EBFileAttribute(NetDBFileAttribute):
    pass


class EBWebAttribute(PIPwareAttribute):
    pass


class MSHEUFileAttribute(NetDBFileAttribute):
    pass


class KZStatAttribute(NetDBFileAttribute):
    pass


class Excel2003XMLFileAttribute(NetDBFileAttribute):
    pass


class JSONMapAttribute(NetDBFileAttribute):
    pass


class JSONListAttribute(NetDBFileAttribute):
    pass


class KZTaskAttribute(PIPwareAttribute):
    pass


class NasdaqHTMLAttribute(PIPwareAttribute):
    pass


class AttributeCollection(MetadataObjectCollection):
    itemType = Attribute


class ExcelFileAttributeCollection(MetadataObjectCollection):
    itemType = ExcelFileAttribute


class HTMLFileAttributeCollection(MetadataObjectCollection):
    itemType = HTMLFileAttribute


class ContentFileAttributeCollection(MetadataObjectCollection):
    itemType = ContentFileAttribute


class NetDBFileAttributeCollection(MetadataObjectCollection):
    itemType = NetDBFileAttribute


class CSVFileAttributeCollection(MetadataObjectCollection):
    itemType = CSVFileAttribute


class MSHEUFileAttributeCollection(MetadataObjectCollection):
    itemType = MSHEUFileAttribute


class KZStatAttributeCollection(MetadataObjectCollection):
    itemType = KZStatAttribute


class PIPwareAttributeCollection(MetadataObjectCollection):
    itemType = PIPwareAttribute


class DCDimensionAttributeCollection(MetadataObjectCollection):
    itemType = DCDimensionAttribute


class EBFileAttributeCollection(MetadataObjectCollection):
    itemType = EBFileAttribute


class EBWebAttributeCollection(MetadataObjectCollection):
    itemType = EBWebAttribute


class Excel2003XMLFileAttributeCollection(MetadataObjectCollection):
    itemType = Excel2003XMLFileAttribute


class JSONMapAttributeCollection(MetadataObjectCollection):
    itemType = JSONMapAttribute


class JSONListAttributeCollection(MetadataObjectCollection):
    itemType = JSONListAttribute


class KZTaskAttributeCollection(MetadataObjectCollection):
    itemType = KZTaskAttribute


class NasdaqHTMLAttributeCollection(MetadataObjectCollection):
    itemType = NasdaqHTMLAttribute

class Entity(Polymorphic, DataObject):
    invalidEntityNameRegex = re.compile("\\.|\"")

    def __str__(self):
        return self.name

    def validate(self):
        super().validate()
        if self.invalidEntityNameRegex.search(self.name):
            raise ValueError("%s.name cannot contain dot or quotation mark." % (
                self.__class__.__name__, ))


class GenericEntity(Entity):
    schema = Entity.schema + (
        SchemaEntry("attributes", AttributeCollection),
    )


class LocalEntity(GenericEntity):
    schema = GenericEntity.schema + (
        SchemaEntry("schemas", SchemaCollection),
        SchemaEntry("partitions", PartitionCollection)
    )


class ViTableLocalEntity(GenericEntity):
    ...


class ReferenceEntity(Entity):
    schema = Entity.schema + (
        SchemaEntry("refreshTime", DateTimeOffset),
        SchemaEntry("source", String),
        SchemaEntry("modelId", String)
    )

    def validate(self):
        super().validate()
        className = self.__class__.__name__
        if not self.source:
            raise ValueError("%s.source is not set." % (className, ))
        if not self.modelId:
            raise ValueError("%s.modelId is not set." % (className, ))
        # TODO: Validate model references


class ResourceEntity(Entity):
    schema = Entity.schema + (
        SchemaEntry("path", String),
    )


class FileEntity(ResourceEntity):
    ...


class WebEntity(ResourceEntity):
    ...


class ExcelFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", ExcelFileAttributeCollection),
    )


class HTMLFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", HTMLFileAttributeCollection),
    )


class ContentFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", ContentFileAttributeCollection),
    )


class HTMLWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", HTMLFileAttributeCollection),
        SchemaEntry("options", OptionCollection)
    )


class NetDBFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", NetDBFileAttributeCollection),
    )


class CSVFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", CSVFileAttributeCollection),
    )


class MSHEUFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", MSHEUFileAttributeCollection),
    )


class KZStatWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", KZStatAttributeCollection),
        SchemaEntry("options", OptionCollection)
    )


class PIPwareWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", PIPwareAttributeCollection),
        SchemaEntry("options", OptionCollection)
    )


class DCDimensionEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", DCDimensionAttributeCollection),
    )


class EBFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", EBFileAttributeCollection),
    )


class EBWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", EBWebAttributeCollection),
        SchemaEntry("options", OptionCollection)
    )


class Excel2003XMLFileEntity(FileEntity):
    schema = FileEntity.schema + (
        SchemaEntry("attributes", Excel2003XMLFileAttributeCollection),
    )


class JSONMapWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", JSONMapAttributeCollection),
    )


class JSONListWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", JSONListAttributeCollection),
    )


class KZTaskWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", KZTaskAttributeCollection),
        SchemaEntry("options", OptionCollection)
    )


class NasdaqHTMLWebEntity(WebEntity):
    schema = WebEntity.schema + (
        SchemaEntry("attributes", NasdaqHTMLAttributeCollection),
        SchemaEntry("options", OptionCollection)
    )

class EntityCollection(MetadataObjectCollection):
    itemType = Entity


class Model(DataObject):
    schema = DataObject.schema + (
        SchemaEntry("application", String),
        SchemaEntry("version", String),
        SchemaEntry("modifiedTime", DateTimeOffset),
        SchemaEntry("culture", String),
        SchemaEntry("referenceModels", ReferenceCollection),
        SchemaEntry("entities", EntityCollection, verbose=True),
        SchemaEntry("relationships", RelationshipCollection)
    )

    currentSchemaVersion = "1.0"

    def __init__(self, name=None):
        super().__init__()
        self.name = name
        self.version = self.currentSchemaVersion

    def __hash__(self):
        entities = '.'.join(sorted([entity.name for entity in self.entities]))
        return hash((entities, ))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return str(json.loads(self.toJson()))

    @classmethod
    def fromJson(cls, value):
        if isinstance(value, str):
            value = json.loads(value)
        elif not isinstance(value, dict):
            value = json.load(value)
        value = SchemaValidator().validate(value, MODELS_META)
        return super(Model, cls).fromJson(value)

    @classmethod
    def fromFile(cls, path):
        with open(path, encoding='utf-8') as f:
            value = f.read()
        return cls.fromJson(value)

    def toJson(self):
        return json.dumps(super().toJson())

    def validate(self, allowUnresolvedModelReferences=True):
        super().validate()
        if self.version != self.currentSchemaVersion:
            raise ValueError("Invalid model version '%s'", self.version)
        if not allowUnresolvedModelReferences:
            for entity in self.entities:
                if isinstance(entity, ReferenceEntity):
                    found = next(
                        (model for model in self.referenceModels if model.id == entity.modelId), None)
                    if found is None:
                        raise ValueError(
                            "ReferenceEntity '%s' doesn't have a reference model" % (entity.name, ))
