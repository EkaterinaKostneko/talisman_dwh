
import datetime as dt
from airflow import DAG

from test.helpers import (run_sql, get_load_id, finish_load, extract_sql, run_python, get_load_params)
from test.config import dwh_db_conn
from test.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get surcharge reports from central db',
    'depend_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_surcharge_reports_test'

# заменить подключение к источнику и поменять схему продюсера на схему источника
def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='test',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'ods'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['test']) as dag:

    # t_truncate = run_sql(
    #     script='dwh_stg_day/_truncate.sql',
    #     task_id='truncate_stg1_dwh'
    # )

    t_load_ods = run_sql(script='ods_surcharge.sql', task_id='load_ods')
    t_load_mart = run_sql(script='mart_surcharge_reports.sql', task_id='load_mart')
    t_finish_load = finish_load()

    t_get_load_params = get_load_params()

    t_get_load_params >> t_extract_sql('surcharge_reports') >> t_load_ods >> t_load_mart >> t_finish_load


