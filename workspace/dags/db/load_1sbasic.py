
import datetime as dt
from airflow import DAG

from db.helpers import (run_sql, finish_load, extract_sql, run_python, get_load_params)
from db.config import dwh_db_conn
from db.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get data from basic 1C tables 1s_const, 1s_journ from db',
    'depend_on_past': False,
    'start_date': dt.datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_1sbasic'

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


with DAG(dag_id, default_args=default_args, schedule_interval='30 19 * * *', catchup=False, tags=['main']) as dag:

    t_truncate = run_sql(
        script='truncate_basic.sql',
        task_id='truncate_stg_dwh')
    t_core_jourdate = run_sql(script='core_1sjourn.sql', task_id='trsfrm_jrnldate')
    t_finish_load = finish_load()
    # t_get_load_id = get_load_id()
    t_get_load_params = get_load_params()

    t_get_load_params >> t_truncate >> [t_extract_sql('_1sconst'), t_extract_sql('_1sjourn')] >> t_core_jourdate >> t_finish_load

