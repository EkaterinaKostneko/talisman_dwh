
import datetime as dt
from airflow import DAG

from central.helpers import (run_sql, get_load_id, finish_load, extract_sql, get_inc, get_load_params)
from central.config import dwh_db_conn
from central.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get data from checkheaders',
    'depend_on_past': False,
    'start_date': dt.datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_checkheaders_inc'

# заменить подключение к источнику и поменять схему продюсера на схему источника
def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='central',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='0 1 * * *', catchup=False,
         tags=['main'], params={'dt_inc_begin': None, 'dt_inc_end': None}) as dag:


  #  t_truncate = run_sql(script='checkheaders_inc_truncate.sql', task_id='truncate_stg_dwh')
    t_finish_load = finish_load()
    t_get_load_params = get_load_params()
    t_load_ods = run_sql(script='ods_checkheaders.sql', task_id='fill_ods')

    t_get_load_params >> get_inc(task_id='get_load_inc') >> t_extract_sql('checkheaders_inc') >> t_load_ods >> t_finish_load

