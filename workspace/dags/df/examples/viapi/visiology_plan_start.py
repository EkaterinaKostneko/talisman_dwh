'''
Created on 17 Jul 2019

@author: pymancer
'''
from datetime import datetime
from airflow import DAG
from operators.visiology_operator import VisiologyAPIOperator  # @UnresolvedImport
from plugins.common.helpers import get_loadplan_id, get_plan_start_url, get_loadplans_url  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Visiology load plan starter',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 7, 2),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_start_visiology_load_plan'
db_id = 'vslg_ro'
plan_name = 'amd_monthly'
visiology_conn_id = 'visiology_ats_outer'
get_plan_task_id = f'{dag_id}.get_plan'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    get_plan = VisiologyAPIOperator(
        task_id=get_plan_task_id,
        resource=get_loadplans_url(db_id),
        visiology_conn_id=visiology_conn_id,
        response_handler=get_loadplan_id,
        handler_args=(plan_name, )
    )

    start_plan = VisiologyAPIOperator(
        task_id=f'{dag_id}.start_plan',
        resource=get_plan_start_url(db_id, get_plan_task_id),
        method='post',
        visiology_conn_id=visiology_conn_id
    )

    get_plan >> start_plan
