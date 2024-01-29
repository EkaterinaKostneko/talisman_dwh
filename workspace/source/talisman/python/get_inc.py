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

dwh_db_connection = getenv('AF_DWH_DB_CONNECTION')
connection = get_decoded_connection(dwh_db_connection)

min_load_date = getenv('AF_MIN_LOAD_DATE')
max_load_date = getenv('AF_MAX_LOAD_DATE')


def delete_table(min_dt, max_dt):
    logger.info(f'Очистка данных из таблицы ods.sc24263_orders c {min_dt} по {max_dt}')
    conn = psycopg2.connect(host=connection['host'],
                            port=connection['port'],
                            database=connection['schema'],
                            user=connection['login'],
                            password=connection['pass'])
    with conn.cursor() as cursor:
        sql_del = '''
            delete 
                from 
            ods.sc24263_orders 
                where sp24278 >= cast(%s as timestamp) and sp24278 <= cast(%s as timestamp)
        '''
        cursor.execute(sql_del, (min_dt, max_dt))
        conn.commit()
    return logger.info('Данные удалены')


def truncate_table(table_name):
    logger.info(f'Очистка таблицы stg_dwh.{table_name}')
    conn = psycopg2.connect(host=connection['host'],
                            port=connection['port'],
                            database=connection['schema'],
                            user=connection['login'],
                            password=connection['pass'])
    with conn.cursor() as cursor:
        cursor.execute(f'''TRUNCATE TABLE stg_dwh.{table_name}''')
        conn.commit()


def run():

    if 'scheduled' in run_id:
        with psycopg2.connect(host=connection['host'],
                              port=connection['port'],
                              database=connection['schema'],
                              user=connection['login'],
                              password=connection['pass']) as conn:
            with conn.cursor() as cursor:
                sql_select = '''
                    select 
                        max(dt_end)
                    from 
                        sys.load
                    where dag_id = 'load_orders_inc' and is_complete = true
                '''
                cursor.execute(sql_select)
                record = cursor.fetchone()

                if len(record) == 0 or record == 'NULL' or record is None:
                    dt_begin = min_load_date
                    dt_end = max_load_date
                    delete_table(dt_begin, dt_end)
                    print(f'{dt_begin};{dt_end}')
                    context_push(value=f'{dt_begin};{dt_end}')
                else:
                    sql_insert = '''
                        insert into sys.load_inc (
                            load_id,
                            dt_begin,
                            dt_end
                        )
                        values (%s, cast(%s as date), cast(%s as date)) 
                        returning dt_begin, dt_end;    
                    '''
                    sql_order = '''
                        select 
                            (max(sp24278)-interval '10 day')::date 
                        from 
                            ods.sc24263_orders
                    '''
                    cursor.execute(sql_order)
                    dt_begin = cursor.fetchone()
                    print(dt_begin[0])
                    dt_end = max_load_date
                    print(dt_end)
                    print(f'производится вставка ({run_id}, {dt_begin[0]}, {dt_end}) в sys.load_inc')
                    print(sql_insert)
                    cursor.execute(sql_insert, (run_id, dt_begin[0], dt_end))
                    print('вставка sys.load_inc успешно')
                    delete_table(dt_begin[0], dt_end)
                    print(f'{dt_begin[0]};{dt_end}')
                    truncate_table('sc24263_orders')
                    context_push(value=f'{dt_begin[0]};{dt_end}')

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
                    sql_select = '''
                        select 
                            max(dt_end)
                        from 
                            sys.load
                        where dag_id = 'load_orders_inc' and is_complete = true
                    '''
                    cursor.execute(sql_select)
                    record = cursor.fetchone()

                    if len(record) == 0 or record == 'NULL' or record is None:
                        dt_begin = min_load_date
                        dt_end = max_load_date
                        delete_table(dt_begin, dt_end)
                        print(f'{dt_begin};{dt_end}')
                    else:
                        sql_insert = '''
                            insert into sys.load_inc (
                                load_id,
                                dt_begin,
                                dt_end
                            )
                            values (%s, to_date(%s::text, 'YYYYMMDD'), to_date(%s::text, 'YYYYMMDD')) 
                            returning dt_begin, dt_end;    
                        '''
                        cursor.execute(sql_insert, (run_id, dt_begin, dt_end))
                        delete_table(dt_begin, dt_end)
                        print(f'{dt_begin};{dt_end}')
                        truncate_table('sc24263_orders')
                        context_push(value=f'{dt_begin};{dt_end}')
        else:
            raise Exception('При запуске дага вручную необходимо передать ему параметры dt_inc_begin, dt_inc_end')


if __name__ == '__main__':
    run()