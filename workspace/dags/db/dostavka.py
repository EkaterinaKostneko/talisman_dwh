import datetime as dt
from airflow import DAG
from db.helpers import (run_sql, get_load_id, finish_load, extract_sql, run_python, get_load_params, step)
from db.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get dostavka from db',
    'depend_on_past': False,
    'start_date': dt.datetime(2022, 8, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_delivery'


def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='delivery',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='50 19 * * *', catchup=False, tags=['main']) as dag:

    t_truncate = run_sql(script='/delivery/truncate_delivery.sql', task_id='truncate_stg')
    t_load_mart = step('load_mart')
    t_load_mart_goods = run_sql(script='/delivery/mart_delivery_goods.sql', task_id='load_goods')
    t_load_mart_couriers = run_sql(script='/delivery/mart_delivery_couriers.sql', task_id='load_couriers')
    t_load_mart_areas = run_sql(script='/delivery/mart_delivery_areas.sql', task_id='load_areas')
    t_finish_load = finish_load()

    t_get_load_params = get_load_params()

    t_get_load_params>> t_truncate >> [t_extract_sql('sc25134_delivery'), t_extract_sql('sc25186_couriers'), \
                          t_extract_sql('sc24500_citysite'), t_extract_sql('sc25152_delivery_area')] \
    >> t_load_mart >> t_load_mart_goods >> t_load_mart_couriers >> t_load_mart_areas >> t_finish_load

