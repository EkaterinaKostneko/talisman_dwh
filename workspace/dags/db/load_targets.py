
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

dag_id = 'load_targets'

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

    t_truncate      = run_sql(
                                script='targets/truncate.sql',
                                task_id='truncate_stg_dwh')
    # t_checkpoint    = run_sql(script='stub.sql',                            task_id='checkpoint')

    t_load_core     = run_sql(script='targets/core_targets.sql',            task_id='load_core')
    t_load_mart     = run_sql(script='targets/mart_targets.sql',            task_id='load_mart_targets')
    t_load_mart_pr  = run_sql(script='targets/mart_targets_pretty.sql',     task_id='load_mart_targets_pretty')

    t_finish_load   = finish_load()
    t_get_load_params = get_load_params()

    t_get_load_params >> \
        t_truncate >> \
        [t_load_core,
        t_load_mart,
        t_load_mart_pr] >> \
    t_finish_load

