"""
Created on Aug 7, 2019

@author: pymancer

Загружает файлы JSON вида
```json
{
    "df_log_level": {
        "strict": true,
        "attributes": [
            {"name": "code", "lk": "true"},
            {"name": "name"}
        ],
        "rows": [
            ["CRITICAL", "Критическая ошибка"],
            ["ERROR",    "Ошибка"],
            ["WARNING",  "Предупреждение"],
            ["INFO",     "Информация"],
            ["DEBUG",    "Отладка"]
        ]
    }
}
```
в таблицы БД и обновляет существующие записи по ключу.  
Расшифровка:
- `df_log_level` - имя таблицы, может быть несколько в одном файле
- `strict` - если true, удаляет из таблицы строки, отсутствующие в файле
- `attributes` - определяет перечень полей таблицы и ключи
- `rows` - данные  

Для исключения устаревших наборов данных используются маски из `.r5ignore` в директории с файла.
"""
import os
import json

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, mapper, clear_mappers

from df.common.exceptions import DFOperatorException  # @UnresolvedImport
from df.common.helpers.logger import Logger
from df.common.helpers.general import (get_conn_string, get_binds,  # @UnresolvedImport
                                       get_files, get_rendered_template, get_db_schema)  # @UnresolvedImport
from df.common.helpers.sql import session_scope, Op  # @UnresolvedImport

log = Logger()

op = Op()
local_binds = {'AF_OP_ID': Op().id, 'AF_OP_INS_ID': Op().ins_id}
binds = get_binds(local_binds=local_binds)

af_json_path = 'AF_JSON_PATH'

json_path = os.getenv(af_json_path)
json_re = os.getenv('AF_JSON_RE')

if not json_path:
    msg = f'Не определена переменная окружения {af_json_path}, отсутствует путь для поиска файлов с данными.'
    raise DFOperatorException(msg)

files = get_files(json_path, reg_exp=json_re)

engine = create_engine(get_conn_string())
Session = sessionmaker(bind=engine)
metadata = MetaData(engine)

schema=get_db_schema()


class CurrentTable:
    pass


def update_row(attrs, row, rows):
    for idx, attr in enumerate(attrs):
        setattr(row, attr['name'], rows[i][idx])


for file in files:
    json_data = get_rendered_template(file, binds=get_binds(local_binds=local_binds))

    log.info(f'Загружаются данные из <{file}>')

    data = json.loads(json_data)

    for entity in data:
        log.info(f'Выполняется сляние данных в таблицу <{entity}>')
        attrs = data[entity]['attributes']
        keys = [{'name': attr['name'], 'idx': idx} for idx, attr in enumerate(attrs) if attr.get('lk')]
        rows = data[entity]['rows']

        ct = Table(entity, metadata, autoload=True, schema=schema)
        clear_mappers()
        mapper(CurrentTable, ct)
        filters = list()

        with session_scope(Session) as s:
            if not data[entity]['strict']:
                for key in keys:
                    key_attr = getattr(CurrentTable, key['name'])
                    key_values = [row[key['idx']] for row in rows]
                    filters.append(key_attr.in_(key_values))
            table_rows = s.query(CurrentTable).filter(*filters).all()

            u_rows = dict()
            u_table_rows = set()
            for row_idx, row in enumerate(rows):
                for table_row_idx, table_row in enumerate(table_rows):
                    if table_row not in u_table_rows:
                        matched = True
                        for k in keys:
                            if getattr(table_row, k['name']) != row[k['idx']]:
                                matched = False
                                break
                        if matched:
                            # нужен update
                            u_rows[row_idx] = table_row_idx
                            u_table_rows.add(table_row_idx)

            for i in range(len(rows)):
                if i in u_rows:
                    # выполняем update
                    u_table_row = table_rows[u_rows[i]]
                    update_row(attrs, u_table_row, rows)
                else:
                    # выполняем insert
                    new_table_row = CurrentTable()
                    update_row(attrs, new_table_row, rows)
                    s.add(new_table_row)

            if data[entity]['strict'] and len(table_rows) > len(u_table_rows):
                # удаляем отсутствующие в файле данных записи
                for i in range(len(table_rows)):
                    if i not in u_table_rows:
                        s.delete(table_rows[i])

engine.dispose()
op.finish()
