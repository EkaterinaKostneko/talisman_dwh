from airflow import DAG
import datetime as dt
from db.helpers import (run_sql, finish_load, extract_sql, run_python, get_load_params)
from test.helpers import run_python


# dwh_db_conn = 'MSSQL_test'
# environment = {'AF_DWH_DB_CONNECTION': serialize_connection(dwh_db_conn)}


# af_dwh_db_connection = getenv('AF_DWH_DB_CONNECTION')
# dwh_db_connection = get_decoded_connection(af_dwh_db_connection)
# db_host = dwh_db_connection['host']
# db_port = dwh_db_connection['port']
# db_database = dwh_db_connection['schema']
# db_user = dwh_db_connection['login']
# db_password = dwh_db_connection['pass']

dag_id = "test_mssql"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": dt.datetime(2022, 12, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    "description": "test",
    "retries": 0
}

# def test_conn():
#     try:
#         conn = pymssql.connect(host=db_host, port=db_port, user=db_user, password=db_password, database=db_database)
#         print("ok")
#     except Exception as e:
#         print(e)

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['test']) as dag:
    t_get_load_params = get_load_params()
    #test_task = run_python('test/conn_mssql.py', task_id='test_task')
