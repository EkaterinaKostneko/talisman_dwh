from datetime import datetime

from airflow import DAG
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.common.helpers import serialize_connection  # @UnresolvedImport
from hooks.email_hook import EmailHook # @UnresolvedImport

# обработчик при падении
def notify(context):
    EmailHook().execute(recipients, context, subject_template=subject_template, content_template=content_template)

default_args = {
    'owner'                 : 'tuser',
    'description'           : 'Email Log on Error',
    'depend_on_past'        : False,
    'start_date'            : datetime(2019, 6, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 0
}

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testmssql')
               }
# получатели
recipients = ['myasnikov@polymedia.ru', 'mikhaylov@polymedia.ru']
# шаблоны (тема, тело) относительно /app/metadata/email
subject_template = 'templates/on_failure_subject.html'
content_template = 'templates/on_failure_log_content.html'

with DAG('df_example_email_log_on_error',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['example']) as dag:
    t1 = DockerOperator(
        task_id='df_example_docker_email_log_on_error',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_SCRIPT_PATH': '/app/ws/source/df/sql/scripts/examples/error.sql'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/sql/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        on_failure_callback=notify
    )
