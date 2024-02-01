
import datetime as dt
from airflow import DAG

from db.helpers import (run_sql, finish_load, extract_sql, run_python, get_load_params)
from db.config import dwh_db_conn
from db.config import src_dwh_db_conn

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
        subdir='db',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='0 1 * * *', catchup=False, tags=['main']) as dag:

  #  t_truncate = run_sql( script='truncate_retail_reports.sql',  task_id='truncate_stg_dwh')
    t_finish_load = finish_load()
    t_trfm_core = run_sql(script='core_assortiment_plain.sql',      task_id='core_plain')
    t_trfm_step1 = run_sql(script='trfm_assortiment_add_return.sql',task_id='trfm_add_returns')
    t_trfm_step2 = run_sql(script='trfm_assortiment_step2.sql',     task_id='trfm_simpl_discont')
    t_trfm_step3 = run_sql(script='trfm_assortiment_step3.sql',     task_id='trfm_union_returns')
    t_trfm_final = run_sql(script='trfm_assortiment_final.sql',     task_id='trfm_filter_resort')
    t_trfm_mart  = run_sql(script='mart_assortiment.sql',      task_id='mart_sales')
    t_get_load_params = get_load_params()


t_get_load_params >> t_trfm_core >> t_trfm_step1 >> \
[t_trfm_step2, t_trfm_step3, t_trfm_final] >> t_trfm_mart >> t_finish_load
