'''
Created on 01 Jul 2019

@author: pymancer
'''
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from sensors.local_file_sensor import LocalFileSensor # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Triggering Excel Extractor',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 7, 2),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_extract_excel_triggering'
upload_directory = '/app/ws/share/df/examples/xlsx/test_sens_upload/'
extract_directory = '/app/ws/share/df/examples/xlsx/test_sens/'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['example']) as dag:

    sensor_task = LocalFileSensor(
        task_id=f'{dag_id}.sense',
        path=upload_directory,
        pattern='.+',
        poke_interval=5)

    process_task = BashOperator(
        task_id=f'{dag_id}.move',
        bash_command=f'mv {upload_directory}* {extract_directory}'
    )
 
    trigger_task = TriggerDagRunOperator(
        task_id=f'{dag_id}.trigger',
        trigger_dag_id='df_example_extract_excel_triggerable'
    )
 
    sensor_task >> process_task >> trigger_task
