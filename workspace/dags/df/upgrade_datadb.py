'''
Created on 29 Jul 2019

@author: pymancer

Создание и обновление системных объектов БД.
Предварительно необходимо создать Airflow Connection
с параметрами подключения к базе хранилища с идентификатором `df` и `extra` = {"schema": "df"},
а в самой базе создать схему `df` с правами на создание в ней таблиц.
'''
from os import getenv
from datetime import datetime
from pathlib import PurePosixPath
from docker.types import Mount
from airflow.models import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import group_chain  # @UnresolvedImport
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner':            'admin',
    'description':      'Data Database Upgrade',
    'depend_on_past':   False,
    'start_date':       datetime(2019, 7, 29),
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          0
}


callables_dir = PurePosixPath('/app/ws/source/df/sql/callables')

# каталоги проливки скриптов типа callables
# списки выполняются последовательно, а их элементы - параллельно
# списки допустимы только на первом уровне
callables = [['common'],
             ['rules', 'json']
             ]

dag_id = 'df_datadb_upgrade'

with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False, tags=['system']) as dag:
    environment = {'AF_EXECUTION_DATE': '{{ ds }}',
                   'AF_TASK_ID': '{{ task.task_id }}',
                   'AF_TASK_OWNER': '{{ task.owner }}',
                   'AF_RUN_ID': '{{ run_id }}',
                   'AF_DWH_DB_CONNECTION': serialize_connection('df')
                   }

    ddl = DockerOperator(
        task_id=f'{dag_id}.ddl',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     # Не менять на DATAFLOW_SCHEMA, это не опечатка. Сделано для поддержки миграции 1
                     # af5f8b97dee0532d6e390e89dfe506b09fa92154
                     **{'AF_DWH_DB_SCHEMA': getenv('AF_DWH_DB_SCHEMA')}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app/ws/source/df/sql/ddl',
        command='alembic upgrade head',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m',
        mount_tmp_dir=False
    )

    callables_tasks = [[ddl]]

    for level in callables:
        parallel = list()

        for location in level:
            suffix = location.replace('/', '_')

            task = DockerOperator(
                task_id=f'{dag_id}.callable_{suffix}',
                image='df_operator:latest',
                api_version='auto',
                auto_remove=True,
                environment={**environment,
                             **{'AF_SCRIPT_PATH': callables_dir.joinpath(location),
                                'AF_AUTOCOMMIT': True,
                                'AF_LOGLEVEL': 'debug'}
                             },
                mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
                working_dir='/app/ws',
                command='python /app/ws/source/df/sql/run_statement.py',
                docker_url='unix://var/run/docker.sock',
                network_mode='bridge',
                cpus=0.25,
                mem_limit='256m',
                mount_tmp_dir=False
            )

            parallel.append(task)
        callables_tasks.append(parallel)

    group_chain(dag, callables_tasks)
