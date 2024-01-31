
import datetime as dt
from airflow import DAG

from trfm.helpers import (run_sql, finish_load, extract_sql, run_python, get_load_params)
from trfm.config import dwh_db_conn
from trfm.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Transform data in dwh',
    'depend_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'trfm_assortiment_plain'

# заменить подключение к источнику и поменять схему продюсера на схему источника
def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='trfm',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'stg_dwh',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'core'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='0 1 * * *', catchup=False, tags=['main']) as dag:

  #  t_truncate = run_sql( script='truncate_retail_reports.sql',  task_id='truncate_stg_dwh')
    t_finish_load = finish_load()
    t_trfm_core = run_sql(script='core_assortiment_plain.sql', task_id='core_plain')
    t_trfm_step1 = run_sql(script='trfm_assortiment_plain_step1.sql', task_id='trfm_step1')
    t_get_load_params = get_load_params()


t_get_load_params >> t_trfm_core >> t_trfm_step1 >> t_finish_load
