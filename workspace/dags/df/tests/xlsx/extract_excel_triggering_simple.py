'''
Created on 01 Jul 2019

@author: pymancer
'''
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from sensors.local_file_sensor import LocalFileSensor # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Triggering Excel Extractor Simple',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 7, 2),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

upload_directory = '/app/ws/share/df/examples/xlsx/test_sens_upload/'
extract_directory = '/app/ws/share/df/examples/xlsx/test_sens/'

with DAG('df_test_extract_excel_triggering_simple',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['test']) as dag:

    sensor_task = LocalFileSensor(
        task_id='df_test_filesensor_extract_excel_triggering_simple',
        path=upload_directory,
        pattern='.+',
        poke_interval=5)

    process_task = BashOperator(
        task_id='df_test_bash_extract_excel_triggering_simple',
        bash_command=f'mv {upload_directory}* {extract_directory}'
    )
 
    trigger_task_1 = TriggerDagRunOperator(
        task_id='df_test_dagrun_extract_excel_triggering_simple_1',
        trigger_dag_id='df_test_extract_excel_triggerable_simple_1'
    )
 
    trigger_task_2 = TriggerDagRunOperator(
        task_id='df_test_dagrun_extract_excel_triggering_simple_2',
        trigger_dag_id='df_test_extract_excel_triggerable_simple_2'
    )
 
    sensor_task >> process_task >> [trigger_task_1, trigger_task_2]
