from os import getenv
import psycopg2
from df.common.helpers.general import Logger, get_decoded_connection

logger = Logger()


def run():
    dag_id = getenv('AF_DAG_ID')
    run_id = getenv('AF_RUN_ID')
    execution_date = getenv('AF_EXECUTION_DATE')
    start_date = getenv('AF_START_DATE')
    dwh_db_connection = getenv('AF_DWH_DB_CONNECTION')
    parent_load_id = getenv('AF_PARENT_LOAD_ID') if getenv('AF_PARENT_LOAD_ID') else None

    params = getenv('AF_PARAMS')

    logger.info(f'AF_DAG_ID={dag_id}, AF_RUN_ID={run_id}, AF_EXECUTION_DATE={execution_date}, '
                f'AF_PARENT_LOAD_ID={parent_load_id}, '
                f'AF_START_DATE={start_date}'
                )

    connection = get_decoded_connection(dwh_db_connection)

    with psycopg2.connect(host=connection['host'],
                          port=connection['port'],
                          database=connection['schema'],
                          user=connection['login'],
                          password=connection['pass']) as conn:
        with conn.cursor() as cursor:
            conn.autocommit = True

            sql_select = '''
                select 
                    id
                from 
                    sys.load
                where run_id = %s and coalesce(parent_id, -10) = coalesce(%s, -10) and dag_id = %s
            '''
            cursor.execute(sql_select, (run_id, parent_load_id, dag_id))
            record = cursor.fetchone()
            if record and record[0]:
                print(f'{record[0]}')
            else:
                sql_insert = '''
                    insert into sys.load (
                        parent_id, dag_id, run_id, dt_execution, dt_start, params
                    )
                    values (%s, %s, %s, %s, %s, %s) 
                    returning id;'''
                cursor.execute(sql_insert, (parent_load_id, dag_id, run_id, execution_date, start_date, params))
                record = cursor.fetchone()
                print(f'{record[0]}')


if __name__ == '__main__':
    run()
