from datetime import datetime
from docker.types import Mount
from airflow.models import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Metadata context upload',
    'depend_on_past'        : False,
    'start_date'            : datetime(2022, 2, 21),
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
               'AF_LOGLEVEL': 'debug'
               }

dag_id = 'df_example_context_upload'

with DAG(
    dag_id,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['example'],
    doc_md='''### Пример DAG'а загрузки метаданных сущностей
- загруженный контекст может использоваться в SQL-скриптах трансформаций
- загрузка выполняется в системные таблицы:
  - df.entity_ctx
  - df.relation_ctx
  - df.attr_ctx''') as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.prepare_entities',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_METADATA_PATH': f'/app/ws/metadata/df/models/examples/sql',
                        'AF_METADATA_RE': 'context_upload_left|context_upload_right'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_create_entity.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False
    )
    t2 = DockerOperator(
        task_id=f'{dag_id}.upload',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/context_upload.sql',
                        'AF_PRODUCER': '/app/ws/metadata/df/models/examples/sql/context_upload_left.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/sql/context_upload_right.json'
                        }
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        cpus=0.25,
        mem_limit='256m',
        mount_tmp_dir=False
    )

    t1 >> t2
