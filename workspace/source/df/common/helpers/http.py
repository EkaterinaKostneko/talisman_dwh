"""
Created on Dec 19, 2020

@author: pymancer@gmail.com
"""
import shutil
import requests

from os import rename
from pathlib import PurePosixPath
from secrets import token_urlsafe
from df.common.constants import CACHE_ROOT  # @UnresolvedImport



def download(url, filename, storage: PurePosixPath, headers=None, verify=True):
    """ Потоковая загрузка файла с url в storage. """
    tmp = CACHE_ROOT.joinpath('tmp')
    tmp.mkdir(parents=True, exist_ok=True)
    tmp_file = token_urlsafe(16)
    tmp_path = tmp.joinpath(tmp_file)

    with requests.get(url, headers=headers, stream=True, verify=verify) as r:
        with open(tmp_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    storage.mkdir(parents=True, exist_ok=True)
    path = storage.joinpath(filename)

    rename(tmp_path, path)

    return path
