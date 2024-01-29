from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


dag_id = 'df_example_trigger_controller'
trigger_task_id = f'{dag_id}.trigger'
external_dag_id = 'df_example_extract_nasdaq_web_html'

with DAG(
    dag_id="df_example_trigger_controller",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="@once",
    tags=['example'],
) as dag:
    trigger = TriggerDagRunOperator(
        task_id=trigger_task_id,
        trigger_dag_id=external_dag_id,
        conf={'dateBegin': '2021-11-22', 'dateEnd': '2021-11-25'}
    )
