import os

from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from docker.types import Mount
from plugins.common.helpers import serialize_connection  # @UnresolvedImport
from hooks.email_hook import EmailHook  # @UnresolvedImport

default_args = {
    'owner': 'airflow',
    'description': 'Email on Bogus Files',
    'depend_on_past': False,
    'start_date': datetime(2019, 6, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# общий префикс переменных окружения для упрощения переключения БД
db_prefix = 'DWH_DB_'
dag_id = 'df_example_docker_email_on_bogus_files'
use_hook = True  # Использовать хук (есть возможность использовать шаблоны) или стандартный оператор

environment = {'AF_EXECUTION_DATE': '{{ ds }}',
               'AF_TASK_ID': '{{ task.task_id }}',
               'AF_TASK_OWNER': '{{ task.owner }}',
               'AF_RUN_ID': '{{ run_id }}',
               'AF_DWH_DB_CONNECTION': serialize_connection('testpostgresql')
               }

# получатели
recipients = ['mikhaylov@polymedia.ru']
# шаблоны (тема, тело) относительно /app/ws/source/df/common/email
subject_template = 'templates/on_failure_subject.html'
content_template = 'templates/on_failure_user_content.html'


def check_bogus_handler(**kwargs):
    path = "/app/ws/share/df/tests/csv/test_failed_folder"  # failedFolder из метаданных, можно передать как переменную
    exist_failed = False
    if os.path.exists(path) and not os.path.isfile(path):
        if os.listdir(path):
            exist_failed = True

    if exist_failed:
        content = '\n'.join(os.listdir(path))
        if use_hook:
            content = f"""Message:<br>
<pre>
{content}
</pre>"""
            kwargs['user_content'] = content
            EmailHook().execute(recipients, kwargs, subject_template=subject_template,
                                content_template=content_template)
        else:
            content = f'''DAG: {kwargs.get("dag").safe_dag_id}<br>
Task: {kwargs.get("ti").task_id}<br>
Run Id: {kwargs.get("run_id")}<br>
Log:<br>
<pre>
{content}
</pre>
            '''

            email = EmailOperator(task_id=f'{dag_id}.send_mail',
                                  to=", ".join(recipients),
                                  subject=f'Airflow task {kwargs.get("ti").task_id} {kwargs.get("ti").state} has bogus files',
                                  html_content=content,
                                  dag=kwargs.get("dag"))
            email.execute(context=kwargs)


with DAG(dag_id, default_args=default_args, schedule_interval=None, catchup=False,
         doc_md='''### Пример DAG'а отправки email-оповещений о наличии файлов, необработанных из-за ошибки.
- загрузка происходит при заданных аннотациях:
  - `ignoreBogusFiles: true`
  - `failedFolder: "/app/ws/share/df/tests/csv/test_failed_folder"`
  - `removeArchivedFolders: true`
  - `archiveFolder: "/app/ws/share/df/tests/csv/test_archive_folder"`
'''
         ) as dag:
    t1 = DockerOperator(
        task_id=f'{dag_id}.extract',
        image='df_operator:latest',
        api_version='auto',
        auto_remove=True,
        environment={**environment,
                     **{'AF_PRODUCER': '/app/ws/metadata/df/models/tests/csv/source_decode_failed.json',
                        'AF_CONSUMER': '/app/ws/metadata/df/models/tests/csv/raw_decode_failed.json'}
                     },
        mounts=[Mount('/app/projects', 'df_projects'), Mount('/app/ws', 'df_workspace')],
        working_dir='/app',
        command='python /app/ws/source/df/excel/run.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    t2 = PythonOperator(task_id=f'{dag_id}.check_bogus',
                        python_callable=check_bogus_handler)

    t1 >> t2
