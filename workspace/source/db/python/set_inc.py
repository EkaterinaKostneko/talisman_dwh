from os import getenv
import psycopg2
from df.common.helpers.general import Logger, get_decoded_connection

import json


def create_select_max_date(entities_inc):
    select_max = [f'select max({entity[1]}) from stg_dwh.{entity[0]}' for entity in entities_inc]
    return '\nunion\n'.join(select_max)


def run():
    # Данный таск запускается в common-даге, поэтому в него передается load_id родителя
    # parent_load_id = getenv('AF_LOAD_ID')

    dwh_db_connection = getenv('AF_DWH_DB_CONNECTION')
    connection = get_decoded_connection(dwh_db_connection)

    # source_inc = getenv('AF_SOURCE_INC')

    entities_inc = json.loads(getenv('AF_ENTITIES_INC'))

    with psycopg2.connect(host=connection['host'],
                          port=connection['port'],
                          database=connection['schema'],
                          user=connection['login'],
                          password=connection['pass']) as conn:
        with conn.cursor() as cursor:
            sql_select = create_select_max_date(entities_inc)
            cursor.execute(sql_select)
            record = cursor.fetchone()
            if record:
                dt_end_estimated = record[0]
            else:
                print('После выполнения ETL-процесса таблицы в ODS пустые')
                dt_end_estimated = None

            sql_update = '''
                update sys.load_inc
                set dt_end_estimated = %s
                where load_id = %s 
            '''

            cursor.execute(sql_update, dt_end_estimated)


if __name__ == '__main__':
    run()