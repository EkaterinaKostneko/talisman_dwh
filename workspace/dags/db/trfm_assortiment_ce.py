
import datetime as dt
from airflow import DAG

from .helpers import (run_sql, finish_load, extract_sql, run_python, get_load_params)
from central.config import dwh_db_conn
from central.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Transform data in dwh',
    'depend_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'trfm_assortiment'

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

def t_run_sql(entity):
    return run_sql(
        source_conn=src_dwh_db_conn,
        subdir='assrt_ce',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='0 1 * * *', catchup=False, tags=['main']) as dag:

  #  t_truncate = run_sql( script='truncate_retail_reports.sql',  task_id='truncate_stg_dwh')
    t_finish_load   = finish_load()
    t_trfm_core     = run_sql(script='asstrt_ce/core_assortiment_ce_plain.sql', task_id='core_plain')
    t_trfm_in       = run_sql(script='asstrt_ce/trfm_assortiment_ce_turn.sql',  task_id='trfm_in')
    t_trfm_step1    = run_sql(script='asstrt_ce/trfm_assortiment_ce_step1.sql', task_id='trfm_1_filter')
    t_trfm_out      = run_sql(script='asstrt_ce/trfm_assortiment_ce_final.sql', task_id='trfm_out')
    t_mart          = run_sql(script='asstrt_ce/mart_assortiment_ce.sql',       task_id='mart_sales')
    t_mart_pretty   = run_sql(script='asstrt_ce/mart_assortiment_ce_pretty.sql',task_id='mart_sales_pretty')

    t_get_load_params = get_load_params()


t_get_load_params >> t_trfm_core >> \
t_trfm_in >> \
t_trfm_step1 >> \
t_trfm_out >> \
[t_mart, t_mart_pretty] >> t_finish_load
