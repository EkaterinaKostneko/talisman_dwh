import datetime as dt
from airflow import DAG

from db.helpers import (run_sql, get_load_id, finish_load, extract_sql, get_inc, get_load_params)
from db.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get internet-orders from db',
    'depend_on_past': False,
    'start_date': dt.datetime(2022, 8, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'orders_inc_v2'


def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='orders',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='0 19 * * *', catchup=False,
         tags=['main'], params={'dt_inc_begin': None, 'dt_inc_end': None}) as dag:

    t_truncate = run_sql(script='/orders/orders_truncate.sql', task_id='truncate_stg')
    t_finish_load = finish_load()
    t_get_load_params = get_load_params()
    t_load_mart = run_sql(script='/orders/mart_orders.sql', task_id='load_mart')
    t_load_ods = run_sql(script='/orders/ods_orders.sql', task_id='fill_ods')
    t_load_total_mart = run_sql(script='/orders/mart_total_orders.sql', task_id='load_total_mart')

    t_get_load_params >> get_inc(task_id='get_load_inc') >> t_truncate >> t_extract_sql('sc24263_orders') >> t_load_ods >> t_load_mart >> t_load_total_mart >> t_finish_load
