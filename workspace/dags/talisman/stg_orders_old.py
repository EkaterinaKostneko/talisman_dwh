
import datetime as dt
from airflow import DAG

from talisman.helpers import (run_sql, get_load_id, finish_load, extract_sql, run_python, get_load_params)
from talisman.config import dwh_db_conn
from talisman.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get internet-orders from db',
    'depend_on_past': False,
    'start_date': dt.datetime(2022, 8, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_orders'

# заменить подключение к источнику и поменять схему продюсера на схему источника
def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='talisman',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['main']) as dag:

    # t_truncate = run_sql(
    #     script='dwh_stg_day/_truncate.sql',
    #     task_id='truncate_stg1_dwh'
    # )
    t_load_mart = run_sql(script='mart_orders.sql', task_id='load_mart')
    t_finish_load = finish_load()

    t_get_load_params = get_load_params()

    t_get_load_params >> t_extract_sql('sc24263_orders') >> t_load_mart >> t_finish_load

