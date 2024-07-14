
import datetime as dt
from airflow import DAG

from central.helpers import (run_sql, get_load_id, finish_load, extract_sql, run_python, get_load_params)
from central.config import dwh_db_conn
from central.config import src_dwh_db_conn

default_args = {
    'owner': 'airflow',
    'description': 'Get sales projects from central',
    'depend_on_past': False,
    'start_date': dt.datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'load_sales_projects'

# заменить подключение к источнику и поменять схему продюсера на схему источника
def t_extract_sql(entity):
    return extract_sql(
        source_conn=src_dwh_db_conn,
        subdir='sales_project',
        entity=entity,
        params={
            'AF_DWH_DB_SCHEMA_PRODUCER': 'dbo',
            'AF_DWH_DB_SCHEMA_CONSUMER': 'stg_dwh'
        }
    )


with DAG(dag_id, default_args=default_args, schedule_interval='00 20 * * *', catchup=False, tags=['main']) as dag:

    t_truncate = run_sql(
        script='sales_project/truncate.sql',
        task_id='truncate_stg_dwh')

    t_load_core = run_sql(script='sales_project/core_sales_project.sql', task_id='load_core')
    t_finish_load = finish_load()

    t_get_load_params = get_load_params()


    t_get_load_params >> t_truncate >> \
    t_load_core >> t_finish_load

