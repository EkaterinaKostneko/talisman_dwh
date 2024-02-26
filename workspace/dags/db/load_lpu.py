
import datetime as dt
from airflow import DAG

from db.helpers import (run_sql, finish_load, extract_sql, run_python, get_load_params)
from db.config import dwh_db_conn
from db.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get data from db',
    'depend_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_lpu'

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


with DAG(dag_id, default_args=default_args, schedule_interval='00 16 * * *', catchup=False, tags=['main']) as dag:

    t_truncate = run_sql(
        script='lpu/truncate_lpu.sql',
        task_id='truncate_stg_dwh')
    t_load_mart = run_sql(script='lpu/mart_lpu.sql', task_id='load_mart')
    t_finish_load = finish_load()
    # t_get_load_id = get_load_id()
    t_get_load_params = get_load_params()

    t_get_load_params >> t_truncate >> \
        [t_extract_sql('sc208_staff'),
         t_extract_sql('sc24297_receipt_bonus'),
         t_extract_sql('sc24313_doctor')]\
    >> t_load_mart >> t_finish_load
