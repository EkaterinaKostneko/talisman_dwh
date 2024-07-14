from os import getenv
import psycopg2
from df.common.helpers.general import Logger, get_decoded_connection
import json
from df.common.helpers.context import context_push, context_pull
import datetime
logger = Logger()

run_id = getenv('AF_RUN_ID')
dag_id = getenv('AF_DAG_ID')
load_id = getenv('AF_LOAD_ID')
params = getenv('AF_PARAMS')
start_date = getenv('AF_START_DATE')

dwh_db_connection = getenv('AF_DWH_DB_CONNECTION')
connection = get_decoded_connection(dwh_db_connection)

min_load_date = getenv('AF_MIN_LOAD_DATE')
max_load_date = getenv('AF_MAX_LOAD_DATE')


def run():

    if 'scheduled' in run_id:
        with psycopg2.connect(host=connection['host'],
                              port=connection['port'],
                              database=connection['schema'],
                              user=connection['login'],
                              password=connection['pass']) as conn:
            with conn.cursor() as cursor:
                sql_select = f'''
                    select 
                        max(dt_end)
                    from 
                        sys.load
                    where dag_id = '{dag_id}' and is_complete = true
                '''
                cursor.execute(sql_select)
                record = cursor.fetchone()

                if len(record) == 0 or record == 'NULL' or record is None:
                    dt_begin = min_load_date
                    dt_end = max_load_date
                    print(f'{dt_begin};{dt_end}')
                    context_push(value=f'{dt_begin};{dt_end}')
                else:
                    sql_insert = '''
                        insert into sys.load_inc_v2 (
                            dag_id,
                            load_id,
                            dt_load,
                            dt_begin,
                            dt_end
                        )
                        values (%s, %s, %s, cast(%s as date) - interval '16 days', cast(%s as date)) 
                        returning dt_begin, dt_end;    
                    '''

                    dt_end = max_load_date
                    cursor.execute(sql_insert, (dag_id, run_id, start_date, record[0], dt_end))
                    ret = cursor.fetchone()
                    dt_begin = ret[0]
                    print(f'производится вставка ({dag_id}, {run_id}, {start_date}, {dt_begin}, {dt_end}) в sys.load_inc')
                    context_push(value=f'{dt_begin};{dt_end}')

    elif 'manual' in run_id:
        dt_begin = f'{json.loads(params).get("dt_inc_begin")}'
        dt_end = f'{json.loads(params).get("dt_inc_end")}'
        if dt_begin and dt_end:
            with psycopg2.connect(host=connection['host'],
                                  port=connection['port'],
                                  database=connection['schema'],
                                  user=connection['login'],
                                  password=connection['pass']) as conn:
                with conn.cursor() as cursor:
                    sql_select = f'''
                        select 
                            max(dt_end)
                        from 
                            sys.load
                        where dag_id = '{dag_id}' and is_complete = true
                    '''
                    cursor.execute(sql_select)
                    record = cursor.fetchone()

                    if len(record) == 0 or record == 'NULL' or record is None:
                        dt_begin = min_load_date
                        dt_end = max_load_date
                        print(f'{dt_begin};{dt_end}')
                    else:
                        sql_insert = '''
                            insert into sys.load_inc_v2 (
                                dag_id,
                                load_id,
                                dt_load,
                                dt_begin,
                                dt_end
                            )
                            values (%s, %s, %s, to_date(%s::text, 'YYYYMMDD'), to_date(%s::text, 'YYYYMMDD')) 
                            returning dt_begin, dt_end;    
                        '''
                        cursor.execute(sql_insert, (dag_id, run_id, start_date, dt_begin, dt_end))
                        print(f'{dt_begin};{dt_end}')
                        context_push(value=f'{dt_begin};{dt_end}')
        else:
            raise Exception('При запуске дага вручную необходимо передать ему параметры dt_inc_begin, dt_inc_end')


if __name__ == '__main__':
    run()