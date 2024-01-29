"""
Created on 28 Feb 2023

@author: vmikhaylov
"""
import os

from collections import defaultdict

from df.common.helpers.general import get_task_id
from df.common.helpers.logger import Logger  # @UnresolvedImport

log = Logger()


class Context:
    def __init__(self):
        self._ctx = defaultdict(lambda: None)

    @property
    def ctx(self):
        return self._ctx

    @ctx.setter
    def ctx(self, value):
        pass

    def push(self, key=None, value=None):
        """ Добавляет в контекст значение
        """
        key = key or get_task_id()
        self.ctx[key] = value
        log.debug(f'Добавлено значение в контекст: {key} => {value}')

    def append(self, key=None, value=None):
        """ Добавляет в контекст одно из значений
        """
        key = key or get_task_id()
        if key not in self.ctx:
            self.ctx[key] = list()
        self.ctx[key].append(value)
        i = len(self.ctx[key]) - 1
        log.debug(f'Добавлено очередное значение в контекст: {key}[{i}] => {value}')

    def pull(self, key=None, default=None):
        """ Возвращает из контекста по ключу значение
        """
        key = key or get_task_id()
        log.debug(f'Получено значение из контекста: {key} => {self.ctx.get(key, default)}')
        return self.ctx.get(key, default)

    def flush(self, task_id=None):
        """ Печатает context в стандартный поток вывод, для передачи значения из таска через XCom
        """
        task_id = task_id or get_task_id()
        log.debug(f'Вывод контекста для {task_id}')
        # После этой строчки не должно быть выводов в stdout, чтобы не перебивать значения контекста
        print(self.ctx.get(task_id))


ctx = Context()


def context_push(**kwargs):
    key = kwargs.get('key')
    value = kwargs.get('value')
    return ctx.push(key=key, value=value)


def context_append(**kwargs):
    key = kwargs.get('key')
    value = kwargs.get('value')
    return ctx.append(key=key, value=value)


def context_pull(**kwargs):
    key = kwargs.get('key')
    default = kwargs.get('default')
    return ctx.pull(key=key, default=default)