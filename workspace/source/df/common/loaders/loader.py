"""
Created on Dec 17, 2020

@author: pymancer@gmail.com
"""
from abc import ABCMeta, abstractmethod


class AbstractLoader(metaclass=ABCMeta):
    def __init__(self, host, token=None, **kwargs):
        self.host = host
        self.token = token

    @abstractmethod
    def find(self, pattern, **kwargs):
        """ Возвращает список объектов контейнера облака, соответствующих переданной маске. """

    @abstractmethod
    def load(self, file, storage, **kwargs):
        """ Загружает и сохраняет объект из контейнера облака.
            На вход получает объект, позволяющий сопоставить его с объектами в контейнере для загрузки.
            Возвращает путь до сохраненного объекта.
        """

    @abstractmethod
    def delete(self, files, **kwargs):
        """ Удаляет объекты из контейнера облака.
            На вход получает список объектов, позволяющий сопоставить их с объектами в контейнере для удаления.
            Возвращает количество удаленных объектов, либо None.
        """
