'''
Created on 24 Oct 2019

@author: pymancer

Перенос файлов в директории, в зависимости от имен самих файлов.
Например, файл АГАТ_20180101-20181231_XI-1к.xlsx из
<upload_directory>/АГАТ_20180101-20181231_XI-1к.xlsx
перенесется в
<archive_directory>/XI-1к/20180101-20181231/АГАТ_20180101-20181231_XI-1к.xlsx
'''
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Fancy File Moving',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 10, 20),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

upload_directory = '/app/ws/share/df/examples/mixedfancy_move/' # директория с файлами, которые нужно перенести
archive_directory = '/app/ws/share/df/examples/mixed/fancy_move/archive/' # корневая директория для создаваемого архива
file_mask = '*.xlsx' # маска для поиска файлов, подлежащих переносу

cmd = """
#!/bin/bash
cd {{ params.upload }}
find . -type f -name "{{ params.mask }}" -print0 | while IFS= read -d '' file
do
    prefix=$(echo $file| cut -s -d'_' -f 1)
    prefix=$(echo $prefix| cut -s -d'/' -f 2)
    infix=$(echo $file| cut -s -d'_' -f 2)
    postfix=$(echo $file| cut -s -d'_' -f 3)
    postfix=$(echo $postfix| cut -s -d'.' -f 1)
    path="{{ params.archive }}$postfix/$infix/"
    mkdir -p $path
    mv "$file" "$path"
done
"""

with DAG('df_example_fancy_move_files',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:

    move_files = BashOperator(
        task_id='df_example_bash_fancy_move_files',
        bash_command=cmd,
        params={'upload': upload_directory, 'archive': archive_directory, 'mask': file_mask},
    )
