"""
Created on Dec 17, 2020

@author: pymancer@gmail.com
"""
from abc import ABCMeta, abstractmethod


class AbstractColumn(metaclass=ABCMeta):
    def __init__(self, name, **kwargs):
        self.name = name


class AbstractContainer(metaclass=ABCMeta):
    def __init__(self, name, columns, **kwargs):
        self.name = name
        self.columns = columns


class AbstractUploader(metaclass=ABCMeta):
    def __init__(self, entity, **kwargs):
        self._conn = None
        self.entity = entity

    @property
    @abstractmethod
    def conn(self):
        """ Подключение к приемнику данных. """

    @abstractmethod
    def upload(self, dataset, **kwargs):
        """ Загружает переданный набор данных в целевой контейнер (таблицу). """

    @abstractmethod
    def create(self, **kwargs):
        """ Создает целевой контейнер (таблицу). """

    @abstractmethod
    def drop(self, **kwargs):
        """ Удаляет целевой контейнер (таблицу). """

    @abstractmethod
    def empty(self, **kwargs):
        """ Удаляет данные из целевого контейнера (таблицы). """

    @abstractmethod
    def get_container(self, **kwargs):
        """ Возвращает объект целевого контейнера (должен иметь атрибуты name и columns). """

    def get_container_name(self, entity=None, **kwargs):
        """ Наименование целевого контейнера. """
        entity = entity or self.entity
        return entity.name

    def commit(self, **kwargs):
        """ Сохраняет загруженный набор данных. По умолчанию не делает ничего. """
        pass

    def dispose(self, **kwargs):
        """ Закрывает соединение, освобождает иные ресурсы. По умолчанию не делает ничего. """
        pass
