from os import getenv
import psycopg2
from df.common.helpers.general import Logger, get_decoded_connection
from datetime import datetime

logger = Logger()


def run():
    load_id = getenv('AF_LOAD_ID')
    dwh_db_connection = getenv('AF_DWH_DB_CONNECTION')
    parent_load_id = getenv('AF_PARENT_LOAD_ID')
    current_date = datetime.now()

    logger.info(f'AF_LOAD_ID={load_id}, AF_PARENT_LOAD_ID={parent_load_id}, current_date={current_date}')

    connection = get_decoded_connection(dwh_db_connection)


    # logger.info(f'''{connection['host']}''')
    # logger.info(f'''{connection['port']}''')
    # logger.info(f'''{connection['schema']}''')
    # logger.info(f'''{connection['login']}''')
    # logger.info(f'''{connection['pass']}''')

    with psycopg2.connect(host=connection['host'],
                          port=connection['port'],
                          database=connection['schema'],
                          user=connection['login'],
                          password=connection['pass']) as conn:
        with conn.cursor() as cursor:
            conn.autocommit = True
            sql_update = ('''
                update sys.load
                set dt_finish = %s, 
                    dt_finish_first = coalesce(dt_finish_first, %s),
                    is_complete = true
                where id = %s;''')
            cursor.execute(sql_update, (current_date, current_date, load_id))


if __name__ == '__main__':
    run()
