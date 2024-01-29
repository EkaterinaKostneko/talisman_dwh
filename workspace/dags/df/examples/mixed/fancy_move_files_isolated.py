'''
Created on 24 Oct 2019

@author: pymancer

Перенос файлов в директории, в зависимости от имен самих файлов.
Например, файл АГАТ_20180101-20181231_XI-1к.xlsx из
<upload_directory>/АГАТ_20180101-20181231_XI-1к.xlsx
перенесется в
<archive_directory>/XI-1к/20180101-20181231/АГАТ_20180101-20181231_XI-1к.xlsx

Перенос выполняется в отдельном докер операторе,
что позволяет выполнять перенос в папки, не только примонтированные к сервису,
для чего необходимо примонтировать необходимые папки к контейнеру.
'''
from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Fancy File Moving Isolated',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 10, 20),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

# параметры архивации
upload_directory = '/app/ws/share/df/examples/mixed/fancy_move/' # директория с файлами, которые нужно перенести
archive_directory = '/app/ws/share/df/examples/mixed/fancy_move/archive/' # корневая директория для создаваемого архива
file_mask = '*.xlsx' # маска для поиска файлов, подлежащих переносу
user = 1000 # id или логин пользователя, имеющего необходимые права доступа, если None - используется root

cmd = f'''
#!/bin/bash
cd {upload_directory}
find . -type f -name "{file_mask}" -print0 | while IFS= read -d "" file
do
    prefix=$(echo $file| cut -s -d"_" -f 1)
    prefix=$(echo $prefix| cut -s -d"/" -f 2)
    infix=$(echo $file| cut -s -d"_" -f 2)
    postfix=$(echo $file| cut -s -d"_" -f 3)
    postfix=$(echo $postfix| cut -s -d"." -f 1)
    path="{archive_directory}$postfix/$infix/"
    mkdir -p $path
    mv "$file" "$path"
done
'''

with DAG('df_example_fancy_move_files_isolated',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    move_files = DockerOperator(
        task_id='df_example_docker_fancy_move_files_isolated',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={
            'AF_EXECUTION_DATE': '{{ ds }}',
            'AF_TASK_OWNER': '{{ task.owner }}',
            'AF_TASK_ID': '{{ task.task_id }}',
            'AF_RUN_ID': '{{ run_id }}'
        },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command=f"""bash -c '{cmd}'""",
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        user=user
    )
