from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'description': 'Nasdaqhtml Extractor',
    'depend_on_past': False,
    'start_date': datetime(2020, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag_id = 'df_example_templated_extract_nasdaq_web_html'
finish_task_id = f'{dag_id}.finish_task'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:
    finish_task = BashOperator(
        task_id=finish_task_id,
        bash_command='echo "DAG finished"'
    )

    finish_task
