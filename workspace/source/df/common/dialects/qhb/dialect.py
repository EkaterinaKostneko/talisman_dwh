"""
Created on Jun 11, 2020

@author: pymancer@gmail.com
"""
from re import match
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2

class QHBDialect(PGDialect_psycopg2):
    """ QHB-specific behavior. Discovered zero functional difference so far. """
    def _get_server_version_info(self, connection):
        v = connection.execute('select version()').scalar()
        m = match(
            r".*(?:QHB) "
            r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:devel|beta)?",
            v,
        )
        if not m:
            raise AssertionError(
                f'Строка <{v}> не соотвествует формату версии СУБД QHB'
            )
        return (9, 6, 18)