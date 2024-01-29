from datetime import datetime
from docker.types import Mount
from airflow.models import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Parameterized DAGRun',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_DAG_ID': '{{ dag.dag_id }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_JOB_ID': '{{ ti.job_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql'),
               'AF_PARAMS': '{{ params }}'
               }

dag_id = 'df_example_parameterized_dagrun'

with DAG(
    dag_id,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example'],
    params={
               'unitId': Param(1, type='integer', minimum=1, maximum=10),
               'dateBegin': '2022-04-01',
               'dateEnd': '2022-12-31'
            },
    doc_md='''### Пример DAG'а с пользовательскими параметрами запуска.
- все параметры со значениями по умолчанию должны быть заданы в `params`
- переданные аргументы доступны операторам в шаблонах
- в контейнеры params следует передавать как `{{ params }}` через переменную `AF_PARAMS`
- если `AF_PARAMS` содержит валидный JSON, то он также доступен в op_ctx.params по `AF_OP_ID`
- помимо `AF_PARAMS` в окружение контейнера должны быть переданы и стандартные переменные
- перечень стандартных переменных - `environment` и их представление в системных таблицах:
  - 'AF_EXECUTION_DATE' (`op.ts`)
  - 'AF_DAG_ID' (`op.proc_id`)
  - 'AF_RUN_ID' (`op.run_id`)
  - 'AF_TASK_ID' (`op_ins.step_id`)
  - 'AF_JOB_ID' (`op_ins.job_id`) - опциональная переменная
'''
) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.prepare_tables',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_METADATA_PATH': f'/app/ws/metadata/df/models/examples/sql',
                        'AF_METADATA_RE': 'tables'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_create_entity.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t2 = DockerOperator(
        task_id=f'{dag_id}.execute',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/basic_sql.sql'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    t1 >> t2
