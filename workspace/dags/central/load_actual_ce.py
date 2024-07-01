
import datetime as dt
from airflow import DAG

from central.helpers import (run_sql, get_load_id, finish_load, extract_sql, run_python, get_load_params)
from central.config import dwh_db_conn
from central.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get actual from checkheaders',
    'depend_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_actual_ce'

# заменить подключение к источнику и поменять схему продюсера на схему источника
def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='actual',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='30 22 * * *', catchup=False, tags=['main']) as dag:

    t_truncate = run_sql(
        script='actual/truncate.sql',
        task_id='truncate_stg_dwh')

    t_load_mart = run_sql(script='actual/marts.sql', task_id='load_mart')
    t_finish_load = finish_load()

    t_get_load_params = get_load_params()


    t_get_load_params >> t_truncate >> \
    t_extract_sql('checkheaders')  >> \
    t_load_mart >> t_finish_load
