import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from operators.template_operator import TemplatableTriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'description': 'Nasdaqhtml From Period Extractor',
    'depend_on_past': False,
    'start_date': datetime(2020, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'df_example_templated_trigger_controller'
trigger_task_id = f'{dag_id}.trigger'
push_task_id = f'{dag_id}.push_params'
external_dag_id = 'df_example_templated_extract_nasdaq_web_html'


@task(task_id=push_task_id)
def push_params(**kwargs):
    """ Получает параметры для передачи в следующие таски """
    return json.dumps({'dateFrom': '2021-11-22', 'dateTo': '2021-11-25', 'ds': 777, 'env1': 'AAA', 'env2': 'XYZ'})


with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    trigger = TemplatableTriggerDagRunOperator(
        task_id=trigger_task_id,
        trigger_dag_id=external_dag_id,
        template_path='df/examples/templates/templated_extract_nasdaq_web_html.py',
        conf=f"{{{{ ti.xcom_pull(task_ids='{push_task_id}', key='return_value') }}}}",
        wait_for_completion=True,
        poke_interval=20
    )

    push_params() >> trigger
