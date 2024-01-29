
import datetime as dt
from airflow import DAG

from db.helpers import (run_sql, get_load_id, finish_load, extract_sql, get_inc, get_load_params)
from db.config import dwh_db_conn
from db.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get data from db',
    'depend_on_past': False,
    'start_date': dt.datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_assortiment_inc'

# заменить подключение к источнику и поменять схему продюсера на схему источника
def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='db',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='0 22 * * *', catchup=False, tags=['main']) as dag:

    t_truncate = run_sql(
        script='truncate_range.sql',
        task_id='truncate_stg_dwh')
    t_load_mart = run_sql(script='mart_assortiment.sql', task_id='load_mart')
    t_load_ods = run_sql(script='ods_dt21203_retail_reports.sql', task_id='fill_ods')
    t_finish_load = finish_load()
    # t_get_load_id = get_load_id()
    t_get_load_params = get_load_params()

    t_get_load_params >> t_truncate >> get_inc(task_id='get_load_inc') >> [t_extract_sql('dt21203_retail_reports'), t_extract_sql('dh21203_retail_reports')] >> t_load_ods >> t_load_mart >> t_finish_load

