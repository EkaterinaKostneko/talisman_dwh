import datetime as dt
from airflow import DAG

from db.helpers import (run_sql, get_load_id, finish_load, extract_sql, get_inc, get_load_params, step)
from db.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get retail reports',
    'depend_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'common_retail_reports_inc'


def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='retail_reports',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='45 19 * * *', catchup=False,
         tags=['main'], params={'dt_inc_begin': None, 'dt_inc_end': None}) as dag:

    t_truncate = run_sql(script='/retail_reports/_truncate.sql', task_id='truncate_stg')
    t_finish_load = finish_load()
    t_get_load_params = get_load_params()
    t_ods_dt = run_sql(script='/retail_reports/ods_dt21203.sql', task_id='ods_dt21203')
    t_ods_dh = run_sql(script='/retail_reports/ods_dh21203.sql', task_id='ods_dh21203')
    t_load_ods = step('ods')
    t_load_dds = run_sql(script='/retail_reports/sales_db.sql', task_id='dds_sales')

    t_get_load_params >> get_inc(task_id='get_load_inc') >> t_truncate >> [t_extract_sql('dt21203_retail_reports'),
                                                                           t_extract_sql('dh21203_retail_reports')] >> \
    t_load_ods >> [t_ods_dt, t_ods_dh] >> t_load_dds >> t_finish_load

