"""
Created on Oct 25, 2021

@author: vmikhaylov
"""
import os
import ftplib
import ftputil
import shutil

from time import sleep
from pathlib import PosixPath

from df.common.exceptions import DFLoaderException  # @UnresolvedImport
from df.common.helpers.logger import Logger
from df.common.helpers.general import Logger, walker  # @UnresolvedImport
from ..loader import AbstractLoader

log = Logger()


class FTPSession(ftplib.FTP):
    def __init__(self, host, userid, password, port):
        """ Аналог конструктора ftplib.FTP, но умеющий подключаться к другому порту """
        ftplib.FTP.__init__(self)
        self.connect(host, port)
        self.login(userid, password)


class TLSFTPSession(ftplib.FTP_TLS):
    def __init__(self, host, userid, password, port=21):
        """ Конструктор для FTPS.
            Для установки уровня логирования использовать self.set_debuglevel(2)
        """
        ftplib.FTP_TLS.__init__(self)
        self.connect(host, port)
        self.login(userid, password)
        self.prot_p()


class FileMeta:
    def __init__(self, name=None, path=None, stat=None):
        self.name = name
        self.path = path
        self.stat = stat


class FTPLoader(AbstractLoader):
    MAX_RECONNECT_COUNT = 3
    RECONNECT_INTERVAL = 5  # sec

    def __init__(self, host, token=None, **kwargs):
        super().__init__(host, token=token, **kwargs)

        host, port = host, 21
        if ':' in host:
            s = host.split(':')
            host, port = s[0], int(s[1])

        self.type = self._get_type(kwargs.get('type'))
        self.remote_host = host
        self.login = kwargs.get('login')
        self.password = token
        self.port = port
        self.time_shift = int(kwargs.get('time_shift')) if kwargs.get('time_shift') else None

        self._reconnect_count = 0
        self._create_host()

        self.level = int(kwargs.get('level')) if kwargs.get('level') else 0  # по умолчанию нет ограничений на глубину
        self.min_level = int(kwargs.get('min_level')) if kwargs.get('min_level') else 1  # копируем с корневой папки

    @staticmethod
    def _get_type(_type):
        if _type:
            _type = _type.lower().strip()
        return _type if _type in ('ftp', 'ftps', '__local') else 'ftp'

    def _get_host_description(self):
        if self.type != '__local':
            return '{}://{}:{}'.format(self.type, self.remote_host, self.port)
        else:
            return 'file://localhost'

    def _create_host(self, raise_err=True):
        log.info(f'(FTPLoader._create_host): пробуем подключиться к {self._get_host_description()}...')

        result = True
        try:
            if self.type == 'ftp':
                self.host = ftputil.FTPHost(self.remote_host, self.login, self.password, port=self.port,
                                            session_factory=FTPSession)
            elif self.type == 'ftps':
                self.host = ftputil.FTPHost(self.remote_host, self.login, self.password, port=self.port,
                                            session_factory=TLSFTPSession)
            elif self.type == '__local':
                self.host = os
        except ftputil.error.PermanentError as e:
            log.error('(FTPLoader._create_host):\n{}'.format(e))
            if raise_err:
                raise e
            result = False

        if result:
            log.info(f'(FTPLoader._create_host): успешно подключено ({self._reconnect_count})')
            self._reconnect_count += 1
            self._set_time_shift()
        return result

    def _set_time_shift(self):
        try:
            if self.time_shift:
                self.host.set_time_shift(self.time_shift)  # смещение в секундах: server_time - client_time
        except Exception as e:
            log.error('(FTPLoader._set_time_shift):\n{}'.format(e))

    def _reconnect(self):
        n = 0
        while n < self.MAX_RECONNECT_COUNT:
            if self._create_host(raise_err=False):
                return True
            sleep(self.RECONNECT_INTERVAL)
            n += 1
        return False

    def find(self, pattern, **kwargs):
        """ Возвращает список файлов удаленного сервера, соответствующих переданной маске """
        return self._get_files(kwargs.get('container', ''), pattern=pattern, level=self.level, min_level=self.min_level)

    def load(self, file, storage, **kwargs):
        overwrite = kwargs.get('overwrite') if kwargs.get('overwrite') is not None else True
        if isinstance(file, dict):
            return self._load(file.get('file'), storage, overwrite=overwrite)
        else:
            return self._load(file, storage, overwrite=overwrite)

    def _load(self, file, storage, overwrite=True):
        """ Загружает и сохраняет файл с удаленного хоста.
            На вход получает имя удаленного файла (объект с информацией) и локальный каталог для сохранения.
            Возвращает путь до сохраненного файла
        """

        if self.host.path.isfile(file):
            local_file = os.path.abspath(os.path.join(os.getcwd(), storage)) + os.path.sep + str(file)
            if not os.path.exists(local_file) or overwrite:
                self._make_parent_dir(local_file)
                if hasattr(self.host, 'download'):
                    try:
                        self.host.download(file, local_file)
                    except ftputil.error.FTPIOError as e:
                        log.error('(FTPLoader._load): FTPOSError:\n{}'.format(e))
                        if self._reconnect():
                            self.host.download(file, local_file)
                        else:
                            log.error('(FTPLoader._load): не удалось переподключиться')
                            raise e
                else:
                    if overwrite and os.path.exists(local_file):
                        os.remove(local_file)
                    shutil.copy2(file, local_file)
            else:
                log.warning('Файл был загружен ранее: {0}'.format(file))
            return local_file
        return None

    def delete(self, files, **kwargs):
        """ Удаляет файлы с удаленного хоста """
        try:
            for file in files:
                file_name = file.get('file')
                if self.host.path.isfile(file_name) or self.host.path.islink(file_name):
                    self.host.remove(file_name)
                elif self.host.path.isdir(file_name):
                    self.host.rmdir(file_name)
        except ftputil.error.FTPOSError as e:
            log.error('(FTPLoader.delete): FTPOSError:\n{}'.format(e))
        except OSError as e:
            log.error('(FTPLoader.delete): OSError:\n{}'.format(e))

    def close(self):
        if self.host and isinstance(self.host, ftputil.FTPHost):
            self.host.close()
            self.host = None

    #
    #  Внутренние методы
    #
    def _get_files(self, path, pattern=None, level=None, min_level=1, with_meta=True):
        """ Возвращает список файлов удаленного хоста, соответствующих переданной маске с учётом глубины поиска
        : param path: рабочая папка на сервере, относительно которого происходит поиск
        : param pattern: шаблон регулярного выражения Python, будут загружены только файлы, соответствующие этому шаблону
        : param level: максимальный уровень дерева каталогов, в которых будет производиться поиск
            (None, 0 - нет ограничений, 1 - корневой каталог)
        : param min_level: минимальный уровень дерева каталогов для скачивания файлов (None, 0, 1 - корневой каталог).
            Используется, чтобы не забирать с корня лишние файлы, т.е. берем файлы только начиная с данного уровня
        : param with_meta: дополнительно возвращается метаинформация по файлам
        """
        path = PosixPath(path)

        if level is None:
            level = 0
        if min_level is None or min_level == 0:
            min_level = 1

        def _is_allowed_level(_root_dir, _dir, _sep, _level):
            _root_dir = str(_root_dir.parts)
            return _dir.count(_sep) - _root_dir.count(_sep) >= _level - 1

        if self.host.path.exists(path):
            if self.host.path.isdir(path):
                files = list()
                for root, __, all_files in walker(path, level=level, walk_obj=self.host):
                    for file in all_files:
                        if pattern.match(file) and _is_allowed_level(path, root, self.host.sep, min_level):
                            self._append_file(files, root, file, with_meta)
            elif self.host.path.isfile(path):
                files = (path,)
            else:
                raise DFLoaderException(f'Путь {path} должен быть файлом или директорией')
        else:
            raise DFLoaderException(f'Путь {path} не существует или к нему нет доступа.')

        return files

    def _append_file(self, files, root, file, with_meta):
        if with_meta:
            files.append(
                {'file': PosixPath(root, file), 'meta': FileMeta(name=self.host.path.basename(PosixPath(root, file)),
                                                                 path=str(PosixPath(root, file)),
                                                                 stat=self.host.stat(PosixPath(root, file)),
                                                                 )})
        else:
            files.append(PosixPath(root, file))

    def _make_parent_dir(self, f_path):
        """ Гарантирует, что родительский каталог пути к файлу существует """
        dir_name = os.path.dirname(f_path)
        if not dir_name:
            return

        while not os.path.exists(dir_name):
            try:
                os.makedirs(dir_name)
            except OSError as e:
                log.error('(FTPLoader._make_parent_dir): {}'.format(e))
                self._make_parent_dir(dir_name)
