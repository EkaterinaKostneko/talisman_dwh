"""
"""
import pendulum

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="df_example_observer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@once",
    tags=['example'],
) as dag:
    trigger = TriggerDagRunOperator(
        task_id="df_example_observer.test_trigger_dagrun",
        trigger_dag_id="df_example_observer_body.bash_task",
        conf={"message": "Hello World"},
    )
