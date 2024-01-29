from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Test Docker Operator',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 1),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=5)
}

with DAG('df_example_docker_test',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    t1 = BashOperator(
        task_id='print_current_date',
        bash_command='date'
    )

    t2 = DockerOperator(
        task_id='df_example_docker_command',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        command='/bin/sleep 10',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t3 = BashOperator(
        task_id='df_example_print_hello',
        bash_command='echo hello world'
    )

    t1 >> t2 >> t3
