from datetime import datetime
from airflow.models import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Simple quality control',
    'depend_on_past'        : False,
    'start_date'            : datetime(2020, 1, 1),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

dag_id = 'df_example_simple_qc'

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_DAG_ID': dag_id,
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_JOB_ID': '{{ ti.job_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql')
               }

with DAG(dag_id,
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:

    prepare_errors = DockerOperator(
        task_id=f'{dag_id}.prepare_errors',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_METADATA_PATH': f'/app/ws/metadata/df/models/examples/rules',
                        'AF_METADATA_RE': 'errors'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run_create_entity.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    prepare_texts = DockerOperator(
        task_id=f'{dag_id}.prepare_texts',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/rules/excel_source_simple.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/rules/excel_raw_simple.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    prepare_dates = DockerOperator(
        task_id=f'{dag_id}.prepare_dates',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/rules/excel_source_simple.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/rules/excel_raw_simple.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    extract = DockerOperator(
        task_id=f'{dag_id}.extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PRODUCER': '/app/ws/metadata/df/models/examples/rules/excel_source_simple.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/examples/rules/excel_raw_simple.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    qc = DockerOperator(
        task_id=f'{dag_id}.qc',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_RULES_PATH': '/app/ws/metadata/df/rules/examples/simple.json',
                        'AF_JOB_ID': '{{ ti.job_id }}',
                        'AF_LOGLEVEL': 'debug'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/qc/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    prepare_errors >> prepare_texts >> prepare_dates >> extract >> qc
