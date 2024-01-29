from os import getenv
import psycopg2
from df.common.helpers.general import Logger, get_decoded_connection
# from plugins.common.helpers import Logger, get_decoded_connection
from datetime import datetime, timezone, timedelta

from df.common.helpers.context import context_push, context_pull


logger = Logger()

# Смещение времени относительно UTC, нужно переделать красиво, как минимум параметризировать
default_timedelta = timedelta(hours=0)
# Шаблон дат Airflow
airflow_pattern = '%Y-%m-%d %H:%M:%S.%f'
# Шаблон дат параметров запуска дага через плагин Soft DAGs
short_pattern = '%Y-%m-%d'


def str_to_date(string, pattern):
    if not string:
        return None
    if string.endswith('+00:00'):
        string = string[:-6]
    return datetime.strptime(string, pattern)


def utc_to_local_date(utc_date, delta=None):
    if not delta:
        delta = default_timedelta
    return utc_date + delta


def utc_str_to_local_date(utc_str, pattern, delta=None):
    return utc_to_local_date(str_to_date(utc_str, pattern), delta)


def run():
    dag_id = getenv('AF_DAG_ID')
    run_id = getenv('AF_RUN_ID')
    execution_date = getenv('AF_EXECUTION_DATE')
    start_date = getenv('AF_START_DATE')
    dwh_db_connection = getenv('AF_DWH_DB_CONNECTION')
    # source_system = getenv('AF_SOURCE_SYSTEM')
    # stage = getenv('AF_STAGE')
    # parent_load_id = getenv('AF_PARENT_LOAD_ID') if getenv('AF_PARENT_LOAD_ID') else None
    sd_date_begin = getenv('AF_SD_DATE_BEGIN')
    sd_date_end = getenv('AF_SD_DATE_END')
    min_load_date = getenv('AF_MIN_LOAD_DATE')

    logger.info(f'AF_DAG_ID={dag_id}, AF_RUN_ID={run_id}, AF_EXECUTION_DATE={execution_date}, '
                # f'AF_START_DATE={start_date}, AF_SD_DATE_BEGIN={sd_date_begin}, AF_SD_DATE_END={sd_date_end}, '
                f'AF_MIN_LOAD_DATE={min_load_date}')

    connection = get_decoded_connection(dwh_db_connection)

    with psycopg2.connect(host=connection['host'],
                          port=connection['port'],
                          database=connection['schema'],
                          user=connection['login'],
                          password=connection['pass']) as conn:
        with conn.cursor() as cursor:
            conn.autocommit = True
            sql_create = '''
                create schema if not exists sys;
                create table if not exists sys."load" (
                    id serial NOT NULL,
                    parent_id text NULL,
                    dag_id text NOT NULL,
                    run_id text NOT NULL,
                    dt_execution timestamp NOT NULL,
                    dt_start timestamp NOT NULL,
                    dt_finish timestamp NULL,
                    dt_finish_first timestamp NULL,
                    dt_begin timestamp NULL,
                    dt_end timestamp NULL,
                    is_complete bool NULL,
                    CONSTRAINT load_pkey PRIMARY KEY (id)
                );'''
            cursor.execute(sql_create)
            if not sd_date_begin and not sd_date_end:
                if not min_load_date:
                    raise Exception(f'Не передано значение AF_MIN_LOAD_DATE')

                sql_insert = '''
                    insert into sys.load (
                        parent_id, dag_id, run_id, dt_execution, dt_start, dt_begin, dt_end
                    )
                    with load as (
                        select 
                            max(dt_end) as max_dt_end
                        from sys.load 
                        where dag_id = %s
                        and dt_end < %s
                        and is_complete
                    )
                    select 
                        'NULL' as parent_id, 
                        %s as dag_id, 
                        %s as run_id, 
                        %s as dt_execution, 
                        %s as dt_start, 
                        coalesce(load.max_dt_end, param.min_load_date) as dt_begin, 
                        %s as dt_end
                    from load
                    right join (select %s::date as min_load_date) param on 1=1
                    returning id, dt_begin, dt_end;'''

                sd_date_end = utc_str_to_local_date(start_date, airflow_pattern)
                logger.info(sd_date_end)
                cursor.execute(sql_insert,
                               (dag_id, sd_date_end, dag_id, run_id, execution_date, start_date,
                                sd_date_end, min_load_date))
                record = cursor.fetchone()

                load_id = record[0]
                dt_begin = record[1]
                dt_end = record[2]

            else:

                if not sd_date_begin:
                    raise Exception(f'Не передано значение AF_SD_DATE_BEGIN')

                if not sd_date_end:
                    sd_date_end = None

                if sd_date_begin and sd_date_end:
                    if str_to_date(sd_date_begin, short_pattern) >= str_to_date(sd_date_end, short_pattern):
                        raise Exception(f'Значение AF_SD_DATE_BEGIN позднее или равно AF_SD_DATE_END')

                sql_insert = '''
                    insert into sys.load (
                        parent_id, dag_id, run_id, dt_execution, dt_start, dt_begin, dt_end, source_system, stage
                    )
                    values ('NULL', %s, %s, %s, %s, %s, %s, %s, %s) 
                    returning id, dt_begin, dt_end;'''
                cursor.execute(sql_insert, (dag_id, run_id, execution_date, start_date, sd_date_begin,
                                            sd_date_end if sd_date_end else utc_str_to_local_date(start_date,
                                                                                                  airflow_pattern),
                                            source_system, stage))
                record = cursor.fetchone()

                load_id = record[0]
                dt_begin = record[1]
                dt_end = record[2]

    # print(f'{load_id};{dt_begin};{dt_end};{stage};{source_system}')
    # context_push(f'{load_id};{dt_begin};{dt_end};{stage};{source_system}')
    context_push(value =load_id)

if __name__ == '__main__':
    run()
