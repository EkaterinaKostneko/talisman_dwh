"""
Created on Mar 2, 2022

@author: pymancer@gmail.com
"""
import logging

from os import getenv


class Logger:
    """ Общий логгер, по умолчанию использующий систему логирования Airflow.
    """
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
        return cls.instance

    def __init__(self, level=None, name=None, formatter=None, logfile=None):
        name = name or 'dataflow'
        self.log = logging.getLogger(name=name)
        if not self.log.hasHandlers():
            level = level or getenv('AF_LOGLEVEL', 'info')
            level = level.upper()
            self.log.setLevel(level.upper())

            if logfile:
                h = logging.FileHandler(logfile)
            else:
                h = logging.StreamHandler()

            h.setLevel(level.upper())
            f = logging.Formatter(formatter)
            h.setFormatter(f)
            self.log.addHandler(h)

    def error(self, msg):
        self.log.error(msg)

    def warning(self, msg):
        self.log.warning(msg)

    def info(self, msg):
        self.log.info(msg)

    def debug(self, msg):
        self.log.debug(msg)
