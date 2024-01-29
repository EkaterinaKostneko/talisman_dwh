"""
Created on 30 Jan 2020

@author: pymancer
"""
from datetime import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from operators.visiology_operator import VisiologyAPIOperator # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Visiology load plan free version',
    'depend_on_past'        : False,
    'start_date'            : datetime(2020, 1, 29),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

db_id = 'db'
plan_name = 'plan2'
visiology_conn_id = 'http_vi_mocker_local'
get_plan_task_id = 'df_test_viplan_getter'
resource_common_base = f'vqadmin/api/databases/{db_id}/loadplans'


def push_loadplan_id_by_name(__, plans, plan_name):
    """ Возвращает id плана по его имени, что автоматически пушит его как XCOM. """
    plan_id = None
    plan_name_lower = plan_name.lower()

    for plan in plans:
        if plan['name'].lower() == plan_name_lower:
            plan_id = plan['id']
            break

    if not plan_id:
        raise AirflowException(f'План <{plan_name}> не найден.')

    return plan_id

with DAG('df_test_viplan_free_version',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['test']) as dag:
    get_plan = VisiologyAPIOperator(
        task_id=get_plan_task_id,
        resource=resource_common_base,
        visiology_conn_id=visiology_conn_id,
        response_handler=push_loadplan_id_by_name,
        handler_args=(plan_name, )
    )

    start_plan = VisiologyAPIOperator(
        task_id='df_test_viplan_free_version_starter',
        resource=(f'{resource_common_base}/{{{{ ti.xcom_pull(task_ids="{get_plan_task_id}") }}}}/start'),
        method='post',
        visiology_conn_id=visiology_conn_id
    )

    get_plan >> start_plan
